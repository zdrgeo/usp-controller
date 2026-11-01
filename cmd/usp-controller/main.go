package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/usp-controller/internal/api"
	"example.com/usp-controller/internal/canary"
	"example.com/usp-controller/internal/config"
	"example.com/usp-controller/internal/controller"
	"example.com/usp-controller/internal/deviceapi"
	"example.com/usp-controller/internal/journal"
	"example.com/usp-controller/internal/mqtt"
)

func main() {
	if err := run(); err != nil {
		slog.Error("controller stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("config", "config.json", "configuration file")
	flag.Parse()
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	token := os.Getenv("USP_API_TOKEN")
	if len(token) < 32 {
		return errors.New("USP_API_TOKEN must contain at least 32 characters")
	}
	j, err := journal.Open(cfg.JournalPath, cfg.JournalMaxBytes)
	if err != nil {
		return err
	}
	defer j.Close()
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := mqtt.New(cfg)
	agents := map[string]string{}
	for id, a := range cfg.Agents {
		agents[id] = a.Topic
	}
	c := controller.New(cfg.EndpointID, agents, transport, j, cfg.MaxPending)
	var requester api.Requester = c
	var receive mqtt.Receiver = c.Receive
	var actorRuntime *canary.Runtime
	var callback *http.Server
	serverErrors := make(chan error, 2)
	if cfg.ActorCanary != nil && cfg.ActorCanary.Enabled {
		actorRuntime, err = canary.New(cfg, transport, c, os.Getenv("DAPR_APP_TOKEN"), os.Getenv("DAPR_API_TOKEN"))
		if err != nil {
			return err
		}
		defer actorRuntime.Close()
		listener, err := net.Listen("tcp", cfg.ActorCanary.CallbackListen)
		if err != nil {
			return err
		}
		callback = &http.Server{Handler: actorRuntime.Host, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
		defer callback.Close()
		go func() { serverErrors <- callback.Serve(listener) }()
		requester = actorRuntime
		receive = actorRuntime.Receive
		// Repair existing actors' reminders through Dapr turns after sidecar startup.
		go func() {
			timer := time.NewTicker(30 * time.Second)
			defer timer.Stop()
			for {
				for _, id := range actorRuntime.Devices {
					if ctx.Err() != nil {
						return
					}
					if _, err := actorRuntime.Client.Invoke(ctx, id, "Activate", nil); err != nil {
						slog.Warn("actor reminder repair", "device", id.AgentID, "error", err)
					}
				}
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
			}
		}()
	}
	if cfg.ReplicaMode() {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				if err := actorRuntime.RefreshPlacement(ctx); err != nil {
					slog.Warn("replica placement readiness", "error", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	if err := transport.Start(ctx, receive); err != nil {
		return err
	}
	timeout := time.Duration(cfg.RequestTimeoutSeconds) * time.Second
	ready := func() bool {
		if cfg.ReplicaMode() {
			return actorRuntime.Ready()
		}
		return transport.Ready() && j.Healthy()
	}
	var handler http.Handler = api.Handler(requester, ready, token, timeout)
	if actorRuntime != nil {
		opts := deviceapi.Options{}
		if policy := cfg.ActorCanary.Twin; policy != nil {
			opts.SiteID = policy.SiteID
			opts.TwinEnabled = true
		}
		handler = actorRuntime.AdmissionGate(deviceapi.Timeout(deviceapi.Handler(handler, actorRuntime.Client, actorRuntime.Devices, token, opts)))
	}
	server := &http.Server{Addr: cfg.Listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: timeout + 15*time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	go func() { serverErrors <- server.ListenAndServe() }()
	slog.Info("USP Controller started", "endpoint", cfg.EndpointID, "listen", cfg.Listen, "agents", len(cfg.Agents))
	select {
	case <-signalCtx.Done():
	case err = <-serverErrors:
	case err = <-transport.Errors():
	}
	if actorRuntime != nil {
		actorRuntime.BeginDrain()
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout+5*time.Second)
	defer shutdownCancel()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		server.Close()
		slog.Warn("HTTP shutdown", "error", shutdownErr)
	}
	if callback != nil {
		if shutdownErr := callback.Shutdown(shutdownCtx); shutdownErr != nil {
			callback.Close()
			slog.Warn("actor callback shutdown", "error", shutdownErr)
		}
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if closeErr := transport.Close(closeCtx); closeErr != nil {
		slog.Warn("MQTT shutdown", "error", closeErr)
	}
	cancel()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("runtime: %w", err)
	}
	return nil
}
