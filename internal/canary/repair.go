package canary

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"example.com/usp-controller/internal/config"
	"example.com/usp-controller/internal/device"
)

// RunReminderRepair only invokes idempotent Activate turns through Dapr. Sweeps
// never overlap; cancellation joins workers before returning. The caller cancels
// this context during drain to interrupt in-flight invocations and the timer.
func (r *Runtime) RunReminderRepair(ctx context.Context) error {
	if !r.repairRunning.CompareAndSwap(false, true) {
		return errors.New("reminder repair already running")
	}
	defer r.repairRunning.Store(false)
	ids := make([]device.DeviceIdentity, 0, len(r.Devices))
	for _, id := range r.Devices {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].AgentID < ids[j].AgentID })
	invoke := func(ctx context.Context, id device.DeviceIdentity) error {
		_, err := r.Client.Invoke(ctx, id, "Activate", nil)
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.draining.Load() {
			return nil
		}
		repairSweep(ctx, ids, r.repairWorkers, invoke, r.draining.Load)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.draining.Load() {
			return nil
		}
		timer := time.NewTimer(30 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// The unbuffered work channel bounds queued work; workers keep draining jobs on
// cancellation/drain so a stop racing a send cannot strand the producer.
func repairSweep(ctx context.Context, ids []device.DeviceIdentity, workers int, invoke func(context.Context, device.DeviceIdentity) error, draining func() bool) {
	workers = min(max(workers, 1), config.MaxReminderRepairWorkers, len(ids))
	jobs := make(chan device.DeviceIdentity)
	var joined sync.WaitGroup
	for i := 0; i < workers; i++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for id := range jobs {
				if ctx.Err() != nil || draining() {
					continue
				}
				if err := invoke(ctx, id); err != nil && ctx.Err() == nil && !draining() {
					slog.Warn("actor reminder repair", "device", id.AgentID, "error", err)
				}
			}
		}()
	}
dispatch:
	for _, id := range ids {
		if ctx.Err() != nil || draining() {
			break
		}
		select {
		case jobs <- id:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	joined.Wait()
}
