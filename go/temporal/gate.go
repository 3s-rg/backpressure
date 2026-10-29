package bptemporal

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	bp "github.com/stano45/backpressure/go"
)

// Defaults for gate retry pacing.
const (
	DefaultMaxGateRetries    = 10
	DefaultGateRetrySleep    = 50 * time.Millisecond
	GateModeEnv              = "BACKPRESSURE_GATE_MODE"
	gateModeLocalActivity    = "local-activity"
	gateModeInline           = "inline"
	defaultAcquireTimeoutS   = 30.0
	defaultAcquireTimeoutEnv = "QUEUED_GATE_ACQUIRE_TIMEOUT_S"
	defaultReleaseTimeout    = 10 * time.Second
)

// GateParams describe one GatedActivity admission attempt.
//
// TenantID, Priority, and RequestID are required. TargetID can be omitted
// when GateOptions.ActivityTargets maps the activity to a target.
type GateParams struct {
	TenantID      string
	Priority      int
	RequestID     string
	TargetID      string
	ResourceCosts map[string]float64
	// MaxGateRetries bounds gate denial + queued-timeout retries combined.
	// Zero means "use DefaultMaxGateRetries", negative means "unbounded".
	MaxGateRetries int
	// GateRetrySleep is the inter-retry sleep on queued-timeout errors.
	// Zero means "use DefaultGateRetrySleep".
	GateRetrySleep time.Duration
}

// LocalActivityOptions exposed for the acquire/release local activities.
//
// Workflow callers can leave this zero, sensible defaults derived from
// QUEUED_GATE_ACQUIRE_TIMEOUT_S are used.
type LocalActivityOptions struct {
	AcquireStartToCloseTimeout time.Duration
	ReleaseStartToCloseTimeout time.Duration
}

// GatedActivity executes a Temporal activity after the backpressure gate
// grants admission. It returns into res via the SDK's standard
// Future.Get(ctx, &res) convention.
//
// Modes (selected at call time via BACKPRESSURE_GATE_MODE):
//   - "local-activity" (default): admission via __bp_acquire local activity,
//     then the user activity is dispatched normally. On failure, __bp_release
//     is best-effort.
//   - "inline": admission + user activity run together inside __bp_inline_run.
//     Requires GateOptions.InlineActivities.
//
// Caller-controlled retry: on backpressure:denied or backpressure:queued_timeout
// the helper sleeps with workflow.Sleep, suffixes the request_id with ":gN",
// and retries until MaxGateRetries is exceeded.
func GatedActivity(
	ctx workflow.Context,
	gate *Gate,
	params GateParams,
	activityType string,
	res any,
	args ...any,
) error {
	targetID, err := resolveTargetID(gate, activityType, params.TargetID)
	if err != nil {
		return err
	}
	mode := strings.ToLower(strings.TrimSpace(os.Getenv(GateModeEnv)))
	if mode == "" {
		mode = gateModeLocalActivity
	}

	if mode == gateModeInline {
		return gatedActivityInline(ctx, params, activityType, targetID, res, args)
	}
	return gatedActivityLocal(ctx, params, activityType, targetID, res, args)
}

func resolveTargetID(gate *Gate, activityType, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if gate == nil {
		return "", errors.New("backpressure: nil Gate passed to GatedActivity")
	}
	if id, ok := gate.activityTargets[activityType]; ok {
		return id, nil
	}
	if len(gate.targets) == 1 {
		for id := range gate.targets {
			return id, nil
		}
	}
	return "", fmt.Errorf(
		"backpressure: cannot resolve target_id for activity %q; pass GateParams.TargetID or configure GateOptions.ActivityTargets",
		activityType,
	)
}

func gatedActivityLocal(
	ctx workflow.Context,
	params GateParams,
	activityType, targetID string,
	res any,
	args []any,
) error {
	acquireTimeoutS := acquireTimeoutSecs()
	acquireLA := time.Duration(acquireTimeoutS+5) * time.Second

	maxRetries := params.MaxGateRetries
	if maxRetries == 0 {
		maxRetries = DefaultMaxGateRetries
	}
	retrySleep := params.GateRetrySleep
	if retrySleep == 0 {
		retrySleep = DefaultGateRetrySleep
	}

	gateRetries := 0
	attemptRequestID := params.RequestID

	for {
		payload := AcquirePayload{
			TargetID:      targetID,
			TenantID:      params.TenantID,
			Priority:      params.Priority,
			ResourceCosts: params.ResourceCosts,
			RequestID:     attemptRequestID,
			TimeoutS:      acquireTimeoutS,
		}

		laCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: acquireLA,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 1,
			},
		})

		var stats StatsDict
		err := workflow.ExecuteLocalActivity(laCtx, ActivityAcquire, payload).Get(ctx, &stats)
		if err != nil {
			retry, ok := shouldRetryGate(err, retrySleep, &gateRetries, maxRetries, params.RequestID, &attemptRequestID)
			if !ok {
				return err
			}
			_ = workflow.Sleep(ctx, retry)
			continue
		}

		// Admission granted: dispatch the user activity normally.
		execErr := workflow.ExecuteActivity(ctx, activityType, args...).Get(ctx, res)
		if execErr != nil {
			// Best-effort release. We use a fresh local-activity context
			// with a tight timeout so a failing Valkey can't stall the
			// workflow.
			releaseCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
				StartToCloseTimeout: defaultReleaseTimeout,
				RetryPolicy: &temporal.RetryPolicy{
					MaximumAttempts: 3,
				},
			})
			_ = workflow.ExecuteLocalActivity(releaseCtx, ActivityRelease, ReleasePayload{
				TargetID:      targetID,
				TenantID:      params.TenantID,
				Priority:      params.Priority,
				ResourceCosts: params.ResourceCosts,
			}).Get(ctx, nil)
			return execErr
		}

		// Best-effort: inject queue stats into a map result.
		injectStatsIntoRes(res, stats, gateModeLocalActivity)
		return nil
	}
}

func gatedActivityInline(
	ctx workflow.Context,
	params GateParams,
	activityType, targetID string,
	res any,
	args []any,
) error {
	acquireTimeoutS := acquireTimeoutSecs()

	maxRetries := params.MaxGateRetries
	if maxRetries == 0 {
		maxRetries = DefaultMaxGateRetries
	}
	retrySleep := params.GateRetrySleep
	if retrySleep == 0 {
		retrySleep = DefaultGateRetrySleep
	}

	gateRetries := 0
	attemptRequestID := params.RequestID

	for {
		payload := InlineRunPayload{
			TargetID:      targetID,
			TenantID:      params.TenantID,
			Priority:      params.Priority,
			ResourceCosts: params.ResourceCosts,
			RequestID:     attemptRequestID,
			TimeoutS:      acquireTimeoutS,
			ActivityName:  activityType,
			ActivityArgs:  args,
		}
		// Inline mode runs as a *regular* activity so the slot is held
		// during the gate wait. The caller controls scheduling and timeout
		// via workflow.WithActivityOptions on ctx.
		err := workflow.ExecuteActivity(ctx, ActivityInlineRun, payload).Get(ctx, res)
		if err != nil {
			retry, ok := shouldRetryGate(err, retrySleep, &gateRetries, maxRetries, params.RequestID, &attemptRequestID)
			if !ok {
				return err
			}
			_ = workflow.Sleep(ctx, retry)
			continue
		}
		return nil
	}
}

// shouldRetryGate inspects an ApplicationError returned by the acquire or
// inline activity and decides whether the workflow should sleep & retry.
// Returns (sleep, true) when caller
// should retry, or (_, false) to propagate the error.
func shouldRetryGate(
	err error,
	defaultSleep time.Duration,
	gateRetries *int,
	maxRetries int,
	baseRequestID string,
	attemptRequestID *string,
) (time.Duration, bool) {
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		return 0, false
	}
	switch appErr.Type() {
	case DeniedErrorType:
		*gateRetries++
		if maxRetries >= 0 && *gateRetries > maxRetries {
			return 0, false
		}
		var retryAfterMs int64
		// Details encoded as []any -> []interface{}, the int64 was supplied
		// as the first detail by the activity.
		_ = appErr.Details(&retryAfterMs)
		sleep := time.Duration(retryAfterMs) * time.Millisecond
		if sleep <= 0 {
			sleep = defaultSleep
		}
		*attemptRequestID = baseRequestID + ":g" + strconv.Itoa(*gateRetries)
		return sleep, true
	case QueuedTimeoutErrorType:
		*gateRetries++
		if maxRetries >= 0 && *gateRetries > maxRetries {
			return 0, false
		}
		*attemptRequestID = baseRequestID + ":g" + strconv.Itoa(*gateRetries)
		return defaultSleep, true
	default:
		return 0, false
	}
}

func acquireTimeoutSecs() float64 {
	if v := os.Getenv(defaultAcquireTimeoutEnv); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return defaultAcquireTimeoutS
}

// injectStatsIntoRes augments the result with queue timing fields when the
// workflow passed a *map[string]any to receive it. Any other type is left untouched (the activity may have
// strongly-typed structs).
func injectStatsIntoRes(res any, stats StatsDict, mode string) {
	m, ok := res.(*map[string]any)
	if !ok || m == nil {
		return
	}
	if *m == nil {
		*m = map[string]any{}
	}
	(*m)["queued_wait_ms"] = stats.WaitMs
	(*m)["queued_polls"] = stats.Polls
	(*m)["queue_enqueued_ms"] = stats.EnqueuedAtMs
	(*m)["queue_granted_ms"] = stats.GrantedAtMs
	(*m)["gate_mode"] = mode
}

// MakeRequestID is a convenience helper that builds the canonical
// "workflowID:seq:attempt" shape the documentation recommends. Workflow code
// should pass a stable, monotonically-increasing seq.
func MakeRequestID(ctx workflow.Context, seq int, attempt int) string {
	info := workflow.GetInfo(ctx)
	return fmt.Sprintf("%s:%d:%d", info.WorkflowExecution.ID, seq, attempt)
}

// Re-export of bp types so callers don't need a second import for the most
// common cases.
type (
	TargetConfig         = bp.TargetConfig
	Constraint           = bp.Constraint
	QueuedAcquireRequest = bp.QueuedAcquireRequest
	AcquireStats         = bp.AcquireStats
)
