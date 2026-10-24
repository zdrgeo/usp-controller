package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/usp-controller/internal/api"
	"example.com/usp-controller/internal/config"
	"example.com/usp-controller/internal/controller"
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
	if err := transport.Start(ctx, c.Receive); err != nil {
		return err
	}
	timeout := time.Duration(cfg.RequestTimeoutSeconds) * time.Second
	ready := func() bool { return transport.Ready() && j.Healthy() }
	server := &http.Server{Addr: cfg.Listen, Handler: api.Handler(c, ready, token, timeout), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: timeout + 15*time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	slog.Info("USP Controller started", "endpoint", cfg.EndpointID, "listen", cfg.Listen, "agents", len(cfg.Agents))
	select {
	case <-signalCtx.Done():
	case err = <-serverErrors:
	case err = <-transport.Errors():
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout+5*time.Second)
	defer shutdownCancel()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		server.Close()
		slog.Warn("HTTP shutdown", "error", shutdownErr)
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
