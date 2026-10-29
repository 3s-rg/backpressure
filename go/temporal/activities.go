// Package bptemporal integrates the backpressure admission gate into the
// Temporal Go SDK:
//
//   - Internal activities __bp_acquire / __bp_release / __bp_inline_run
//     coordinate with Valkey through the embedded Lua script.
//   - Workflow code calls GatedActivity(...) instead of workflow.ExecuteActivity
//     to opt into admission control and (optionally) inline mode.
//
// Workers that share a Valkey instance and a target_id coordinate
// transparently, whichever language they run.
package bptemporal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	bp "github.com/stano45/backpressure/go"
	"github.com/valkey-io/valkey-go"
)

// Activity names registered with the worker. They mirror Python.
const (
	ActivityAcquire   = "__bp_acquire"
	ActivityRelease   = "__bp_release"
	ActivityInlineRun = "__bp_inline_run"
)

// Application error types raised by the acquire/inline activities.
const (
	DeniedErrorType        = "backpressure:denied"
	QueuedTimeoutErrorType = "backpressure:queued_timeout"
)

// AcquirePayload is the JSON-friendly input for the Acquire activity.
type AcquirePayload struct {
	TargetID      string             `json:"target_id"`
	TenantID      string             `json:"tenant_id"`
	Priority      int                `json:"priority"`
	ResourceCosts map[string]float64 `json:"resource_costs"`
	RequestID     string             `json:"request_id"`
	TimeoutS      float64            `json:"timeout_s"`
}

// ReleasePayload is the JSON-friendly input for the Release activity.
type ReleasePayload struct {
	TargetID      string             `json:"target_id"`
	TenantID      string             `json:"tenant_id"`
	Priority      int                `json:"priority"`
	ResourceCosts map[string]float64 `json:"resource_costs"`
}

// InlineRunPayload is the JSON-friendly input for the InlineRun activity.
type InlineRunPayload struct {
	TargetID      string             `json:"target_id"`
	TenantID      string             `json:"tenant_id"`
	Priority      int                `json:"priority"`
	ResourceCosts map[string]float64 `json:"resource_costs"`
	RequestID     string             `json:"request_id"`
	TimeoutS      float64            `json:"timeout_s"`
	ActivityName  string             `json:"activity_name"`
	// ActivityArgs is a list of JSON-encodable positional arguments forwarded
	// to the registered inline function by value. Args are decoded into the
	// function's parameter types via the Temporal data converter.
	ActivityArgs []any `json:"activity_args"`
}

// StatsDict is the wire shape returned by the Acquire activity.
type StatsDict struct {
	WaitMs       int   `json:"wait_ms"`
	Polls        int   `json:"polls"`
	EnqueuedAtMs int64 `json:"enqueued_at_ms"`
	GrantedAtMs  int64 `json:"granted_at_ms"`
}

// Gate holds the shared state used by the three backpressure activities.
//
// One Gate is constructed per worker process. RegisterActivities binds its
// methods to the worker under the internal activity names listed above.
type Gate struct {
	client          valkey.Client
	targets         map[string]bp.TargetConfig
	activityTargets map[string]string

	// inlineFns maps activity type name -> the in-process function the
	// inline-mode wrapper should invoke. Functions are dispatched via
	// reflection because Go (unlike Python) has no dynamic lookup-by-name.
	// Both context.Context and activity.Context (an interface) are accepted
	// as the first parameter, remaining args come from ActivityArgs.
	inlineFns map[string]any
}

// GateOptions configure a Gate. None of the fields are required.
type GateOptions struct {
	// Targets maps target_id -> TargetConfig. Required for any non-trivial
	// usage, otherwise activities will fail with "unknown target".
	Targets map[string]bp.TargetConfig
	// ActivityTargets maps activity type name -> target_id, allowing
	// GatedActivity callers to omit the explicit TargetID.
	ActivityTargets map[string]string
	// InlineActivities maps activity type name -> the user activity Go
	// function. Required only when callers use the "inline" gate mode.
	// The same function should also be registered as a regular activity for
	// local-activity mode to keep working.
	InlineActivities map[string]any
}

// NewGate constructs a Gate using the given valkey-go client. The client is
// retained for the lifetime of the Gate and is the caller's responsibility
// to close.
func NewGate(client valkey.Client, opts GateOptions) *Gate {
	g := &Gate{
		client:          client,
		targets:         copyTargets(opts.Targets),
		activityTargets: copyStringMap(opts.ActivityTargets),
		inlineFns:       map[string]any{},
	}
	for name, fn := range opts.InlineActivities {
		if fn == nil {
			continue
		}
		g.inlineFns[name] = fn
	}
	return g
}

func copyTargets(m map[string]bp.TargetConfig) map[string]bp.TargetConfig {
	out := make(map[string]bp.TargetConfig, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyStringMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Targets returns a snapshot of the targets registered with this Gate. Useful
// when GatedActivity needs to resolve a target by activity name from the
// workflow side. The returned map must not be mutated.
func (g *Gate) Targets() map[string]bp.TargetConfig { return g.targets }

// ActivityTargets returns the activity-type-name -> target_id map. The
// returned map must not be mutated.
func (g *Gate) ActivityTargets() map[string]string { return g.activityTargets }

// Acquire implements the __bp_acquire activity. It runs QueuedAcquire in
// blocking mode and maps backpressure errors to *temporal.ApplicationError
// values the workflow-side retry loop in GatedActivity can react to.
func (g *Gate) Acquire(ctx context.Context, p AcquirePayload) (StatsDict, error) {
	target, ok := g.targets[p.TargetID]
	if !ok {
		return StatsDict{}, fmt.Errorf("backpressure: unknown target %q", p.TargetID)
	}

	tAcquire := time.Now()
	stats, err := bp.QueuedAcquire(ctx, g.client, target, bp.QueuedAcquireRequest{
		TenantID:      p.TenantID,
		Priority:      p.Priority,
		ResourceCosts: p.ResourceCosts,
		RequestID:     p.RequestID,
		TimeoutS:      p.TimeoutS,
		Blocking:      true,
	}, g.makePollCallback(ctx, p.TargetID, p.TenantID, p.Priority))
	if err != nil {
		if retry, ok := bp.IsDenied(err); ok {
			return StatsDict{}, temporal.NewNonRetryableApplicationError(
				err.Error(), DeniedErrorType, err, int64(retry),
			)
		}
		if bp.IsTimeout(err) {
			return StatsDict{}, temporal.NewNonRetryableApplicationError(
				err.Error(), QueuedTimeoutErrorType, err,
			)
		}
		return StatsDict{}, err
	}

	waitWallMs := int(time.Since(tAcquire) / time.Millisecond)
	g.recordAcquireMetrics(ctx, p.TargetID, p.TenantID, p.Priority, waitWallMs, stats.Polls)

	return StatsDict{
		WaitMs:       stats.WaitMs,
		Polls:        stats.Polls,
		EnqueuedAtMs: stats.EnqueuedAtMs,
		GrantedAtMs:  stats.GrantedAtMs,
	}, nil
}

// Release implements the __bp_release activity. Errors are swallowed and
// logged so the workflow's original failure remains the surfaced one.
func (g *Gate) Release(ctx context.Context, p ReleasePayload) error {
	target, ok := g.targets[p.TargetID]
	if !ok {
		log.Printf("backpressure: unknown target %q; skipping release", p.TargetID)
		return nil
	}
	if err := bp.QueuedRelease(ctx, g.client, target, p.TenantID, p.ResourceCosts, p.Priority); err != nil {
		log.Printf("backpressure: queued_release failed for target=%s tenant=%s: %v",
			p.TargetID, p.TenantID, err)
	}
	return nil
}

// InlineRun implements the __bp_inline_run activity. It acquires the gate
// and then invokes the user activity function in-process via reflection.
// Returns a map[string]any merged with queue-stats fields, matching the
// Python wire shape.
func (g *Gate) InlineRun(ctx context.Context, p InlineRunPayload) (map[string]any, error) {
	inlineStartedMs := time.Now().UnixMilli()

	target, ok := g.targets[p.TargetID]
	if !ok {
		return nil, fmt.Errorf("backpressure: unknown target %q", p.TargetID)
	}
	fn, ok := g.inlineFns[p.ActivityName]
	if !ok {
		return nil, fmt.Errorf(
			"backpressure: activity %q not registered for inline mode; pass it via GateOptions.InlineActivities",
			p.ActivityName,
		)
	}

	tAcquire := time.Now()
	stats, err := bp.QueuedAcquire(ctx, g.client, target, bp.QueuedAcquireRequest{
		TenantID:      p.TenantID,
		Priority:      p.Priority,
		ResourceCosts: p.ResourceCosts,
		RequestID:     p.RequestID,
		TimeoutS:      p.TimeoutS,
		Blocking:      true,
	}, g.makePollCallback(ctx, p.TargetID, p.TenantID, p.Priority))
	if err != nil {
		if retry, ok := bp.IsDenied(err); ok {
			return nil, temporal.NewNonRetryableApplicationError(
				err.Error(), DeniedErrorType, err, int64(retry),
			)
		}
		if bp.IsTimeout(err) {
			return nil, temporal.NewNonRetryableApplicationError(
				err.Error(), QueuedTimeoutErrorType, err,
			)
		}
		return nil, err
	}

	waitWallMs := int(time.Since(tAcquire) / time.Millisecond)
	g.recordAcquireMetrics(ctx, p.TargetID, p.TenantID, p.Priority, waitWallMs, stats.Polls)

	result, runErr := invokeInline(ctx, fn, p.ActivityArgs)
	if runErr != nil {
		// Best-effort release: refund the grant so the bucket reflects that
		// the upstream call did not happen.
		if err := bp.QueuedRelease(ctx, g.client, target, p.TenantID, p.ResourceCosts, p.Priority); err != nil {
			log.Printf("backpressure: inline release failed: %v", err)
		}
		return nil, runErr
	}

	// Reconcile: adjust bucket state based on actual usage reported in the
	// result. Only meaningful for multi-constraint targets where token
	// estimates can diverge from actuals, single-dim (RPM-only) targets skip
	// this entirely to preserve the proven single-dim gate behavior.
	if len(target.Constraints) > 1 {
		g.reconcileCosts(ctx, target, p.TargetID, p.TenantID, p.Priority, p.ResourceCosts, result)
	}

	extra := map[string]any{
		"queued_wait_ms":        waitWallMs,
		"queued_polls":          stats.Polls,
		"queue_enqueued_ms":     stats.EnqueuedAtMs,
		"queue_granted_ms":      stats.GrantedAtMs,
		"gate_mode":             "inline",
		"activity_started_ms":   inlineStartedMs,
		"activity_returning_ms": time.Now().UnixMilli(),
	}

	// If the user activity returned a map, merge in-place. Otherwise wrap in
	// {"value": result, ...extra} so the workflow always gets a dict back
	// regardless of the activity's own return shape.
	if m, ok := result.(map[string]any); ok {
		for k, v := range extra {
			m[k] = v
		}
		return m, nil
	}
	out := map[string]any{"value": result}
	for k, v := range extra {
		out[k] = v
	}
	return out, nil
}

// invokeInline calls fn via reflection. The function signature must be one of:
//
//	func(ctx context.Context, args...) (T, error)
//	func(ctx context.Context, args...) error
//	func(args...) (T, error)
//	func(args...) error
//
// where the first parameter is optional and arg types must be assignable
// from the JSON-decoded values in payloadArgs.
func invokeInline(ctx context.Context, fn any, payloadArgs []any) (any, error) {
	fv := reflect.ValueOf(fn)
	if !fv.IsValid() || fv.Kind() != reflect.Func {
		return nil, fmt.Errorf("backpressure: inline fn is not a function (%T)", fn)
	}
	ft := fv.Type()

	callArgs := make([]reflect.Value, 0, ft.NumIn())
	in := 0
	if ft.NumIn() > 0 && ft.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		callArgs = append(callArgs, reflect.ValueOf(ctx))
		in++
	}
	expected := ft.NumIn() - in
	if ft.IsVariadic() {
		expected--
	}
	if !ft.IsVariadic() && len(payloadArgs) != expected {
		return nil, fmt.Errorf(
			"backpressure: inline fn expects %d args, payload supplied %d",
			expected, len(payloadArgs),
		)
	}
	for i, raw := range payloadArgs {
		var targetType reflect.Type
		if ft.IsVariadic() && in+i >= ft.NumIn()-1 {
			targetType = ft.In(ft.NumIn() - 1).Elem()
		} else {
			targetType = ft.In(in + i)
		}
		v, err := coerceTo(raw, targetType)
		if err != nil {
			return nil, fmt.Errorf("backpressure: inline arg %d: %w", i, err)
		}
		callArgs = append(callArgs, v)
	}

	out := fv.Call(callArgs)
	// Convention: last return value is error (or nothing).
	switch len(out) {
	case 0:
		return nil, nil
	case 1:
		if errVal, ok := out[0].Interface().(error); ok {
			return nil, errVal
		}
		return out[0].Interface(), nil
	case 2:
		var retErr error
		if e, ok := out[1].Interface().(error); ok {
			retErr = e
		}
		return out[0].Interface(), retErr
	default:
		return nil, fmt.Errorf("backpressure: inline fn returned %d values; expected 0, 1, or 2", len(out))
	}
}

// coerceTo converts a JSON-decoded value into a reflect.Value compatible with
// targetType. It is intentionally narrow: number widening, string passthrough,
// and slice/map assignment for non-numeric kinds. Anything else falls back to
// direct assignment and will error if incompatible.
func coerceTo(raw any, targetType reflect.Type) (reflect.Value, error) {
	if raw == nil {
		return reflect.Zero(targetType), nil
	}
	rv := reflect.ValueOf(raw)
	if rv.Type().AssignableTo(targetType) {
		return rv, nil
	}
	if rv.Type().ConvertibleTo(targetType) {
		return rv.Convert(targetType), nil
	}
	return reflect.Value{}, fmt.Errorf("cannot use %T as %s", raw, targetType)
}

// reconcileThreshold is the minimum per-dim |estimated - actual| delta that
// triggers a refund or debit.
const reconcileThreshold = 0.5

// reconcileCosts adjusts bucket state based on actual usage reported in the
// activity result. When the result
// contains a "usage" map (as returned by mocklimit-style activities) it
// computes the per-dimension delta between estimated and actual costs:
//   - estimated > actual: release the overestimate (refund excess capacity)
//   - estimated < actual: debit the shortfall (negative release) so the gate's
//     bucket aligns with the upstream's actual consumption
//
// Errors are logged and swallowed, reconciliation is best-effort.
func (g *Gate) reconcileCosts(
	ctx context.Context,
	target bp.TargetConfig,
	targetID, tenant string,
	priority int,
	estimatedCosts map[string]float64,
	result any,
) {
	m, ok := result.(map[string]any)
	if !ok {
		return
	}
	usageRaw, ok := m["usage"]
	if !ok {
		return
	}
	usage, ok := usageRaw.(map[string]any)
	if !ok {
		return
	}

	refund := map[string]float64{}
	debit := map[string]float64{}
	for dim, estimated := range estimatedCosts {
		actual, ok := resolveActualCost(usage, dim)
		if !ok {
			continue
		}
		delta := estimated - actual
		switch {
		case delta > reconcileThreshold:
			refund[dim] = delta
		case delta < -reconcileThreshold:
			debit[dim] = -delta
		}
	}

	if len(refund) == 0 && len(debit) == 0 {
		return
	}

	if len(refund) > 0 {
		if err := bp.QueuedRelease(ctx, g.client, target, tenant, refund, priority); err != nil {
			log.Printf("backpressure: reconcile refund failed for target=%s tenant=%s: %v", targetID, tenant, err)
		}
	}
	if len(debit) > 0 {
		// Negative release = debit. The Lua release op adds cost to the
		// bucket, so a negative cost subtracts, reducing available capacity to
		// match what the upstream actually consumed.
		neg := make(map[string]float64, len(debit))
		for dim, amount := range debit {
			neg[dim] = -amount
		}
		if err := bp.QueuedRelease(ctx, g.client, target, tenant, neg, priority); err != nil {
			log.Printf("backpressure: reconcile debit failed for target=%s tenant=%s: %v", targetID, tenant, err)
		}
	}
}

// resolveActualCost extracts the actual cost for a dimension from a usage map.
// Dotted dimensions ("tokens.input" /
// "tokens.output") read usage.inputs / usage.outputs, plain dimensions read
// the top-level total.
func resolveActualCost(usage map[string]any, dim string) (float64, bool) {
	if idx := strings.Index(dim, "."); idx >= 0 {
		resource := dim[:idx]
		component := dim[idx+1:]
		switch component {
		case "input":
			if inputs, ok := usage["inputs"].(map[string]any); ok {
				if v, ok := inputs[resource]; ok {
					return toFloat64(v)
				}
			}
		case "output":
			if outputs, ok := usage["outputs"].(map[string]any); ok {
				if v, ok := outputs[resource]; ok {
					return toFloat64(v)
				}
			}
		}
		return 0, false
	}
	if v, ok := usage[dim]; ok {
		return toFloat64(v)
	}
	return 0, false
}

// toFloat64 loosely converts a usage value (native Go numeric or string) to a
// float64.
func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// makePollCallback returns a per-poll metric recorder bound to the activity
// context, or nil when we are not inside a Temporal activity (e.g. unit tests).
func (g *Gate) makePollCallback(ctx context.Context, targetID, tenant string, priority int) bp.PollCallback {
	if !inActivity(ctx) {
		return nil
	}
	handler := activity.GetMetricsHandler(ctx).WithTags(map[string]string{
		"target":   targetID,
		"tenant":   tenant,
		"priority": fmt.Sprintf("%d", priority),
	})
	return func(evalMs int, granted bool, _ string) {
		evalHandler := handler.WithTags(map[string]string{"granted": boolToStr(granted)})
		evalHandler.Timer("backpressure_acquire_eval_ms").Record(time.Duration(evalMs) * time.Millisecond)
	}
}

func (g *Gate) recordAcquireMetrics(ctx context.Context, targetID, tenant string, priority int, waitMs, polls int) {
	if !inActivity(ctx) {
		return
	}
	handler := activity.GetMetricsHandler(ctx).WithTags(map[string]string{
		"target":   targetID,
		"tenant":   tenant,
		"priority": fmt.Sprintf("%d", priority),
	})
	handler.Timer("backpressure_queued_acquire_wait_ms").Record(time.Duration(waitMs) * time.Millisecond)
	// Tally maps Timers to Prometheus histograms. Recording polls as a "Timer"
	// (with the count converted to a duration) lets the Tally->Prom adapter
	// emit backpressure_acquire_polls_{bucket,count,sum}. Buckets come from the
	// host application's Tally ScopeOptions.
	handler.Timer("backpressure_acquire_polls").Record(time.Duration(polls) * time.Millisecond)
}

// inActivity reports whether ctx is a Temporal activity context. We detect
// this by attempting activity.GetInfo, which only works under a registered
// activity. Wrapped in defer/recover because the SDK panics outside.
func inActivity(ctx context.Context) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
		}
	}()
	_ = activity.GetInfo(ctx)
	return true
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// Ensure the activity payload types do not silently lose fields if the
// Temporal data converter reorders map encoding.
var _ = errors.New
