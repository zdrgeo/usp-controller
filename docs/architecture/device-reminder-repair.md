# Bounded reminder repair for Device Actor cohorts

The opt-in runtime can configure a bounded reminder-repair worker pool:

```json
{
  "actor_canary": {
    "reminder_repair_workers": 4
  }
}
```

This fragment belongs inside an otherwise valid enabled actor configuration.
Omission or zero uses one worker; nonzero values must be 1–16 and require actor
mode. Legacy remains the default runtime, and example configuration does not
enable parallel repair.

The first sweep starts after callback hosting is available. Each sweep takes the
fixed provisioned inventory in lexical device order and invokes each device's
idempotent `Activate` through Dapr once. Workers share an unbuffered work channel;
there is no goroutine per device or durable secondary queue. Active invocations
are bounded by the worker count and inventory size. A slow or failed device can
occupy one worker, while other workers continue with the cohort. Failed devices
are logged and revisited on a later sweep, not retried immediately within the pass.
The existing client invocation timeout still applies.

Sweeps never overlap within one Runtime, and a second concurrent
`RunReminderRepair` is rejected. The next sweep waits 30 seconds after the previous
one finishes. There is no accumulated ticker catch-up burst after a slow sweep.
Inventory is fixed for the runtime lifetime; provisioning changes require the
existing reviewed restart procedure.

## Execution and shutdown boundaries

Repair invokes only `Activate`. It loads/validates actor state and repairs the
existing reminder registration. It does not invoke Tick, claim SQL work, publish
MQTT, acknowledge uncertainty or replay an attempted command. Existing reminder
staggering and retry identity are preserved by the registration implementation.
Dapr continues to serialize turns per device.

Drain checks stop scheduling new repair work. The application also cancels the
repair context at shutdown, interrupting client requests and the between-sweep
timer, then joins all local workers before releasing runtime resources. Library
callers of `RunReminderRepair` own this context and must cancel it during drain
for prompt return; BeginDrain alone does not interrupt a sleeping timer or an
already active request.

Client cancellation is not a guarantee that an already-entered remote Activate
turn stops. It may finish after the client leaves. Its idempotent, non-dispatching
contract remains the safety boundary. Local workers do not start queued activations
after observing cancellation/drain, and cancellation cannot strand the producer
waiting for a worker that has exited.

This pool bounds reminder repair only. User requests, fleet projection refreshes,
placement readiness probes and Dapr's own reminder callbacks remain independent.
Multiple hosts each have their own pool. This is not a global activation quota,
command attempt budget, worker ownership lease or full-cohort readiness guarantee.
Existing readiness checks do not fan out across the inventory or wait for a
complete repair sweep.

## Validation and compatibility

Snapshot schema 9, reminder format, backend fencing and legacy behavior are
unchanged. Existing Activate metrics and per-device repair warnings remain the
observability surfaces; no new metric labels or HTTP control route are added.

Validation on 2026-11-14: the full race-enabled Go suite and `go vet` passed.
Focused checks cover the worker bound, progress after one failed device without
within-sweep retry, cancellation joining workers without queued calls, activation
through the actual Dapr client, rejection of overlapping runners and runtime
configuration wiring. Long infrastructure fault drills were not repeated for
this host-local lifecycle change.

The repair sweep delay is independent of the
[configured maintenance cadence](device-maintenance-cadence.md). A configured
period mismatch remains a repair error; this pool does not migrate reminders.
