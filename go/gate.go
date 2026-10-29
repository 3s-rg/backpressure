package backpressure

import (
	"context"
	"crypto/sha1" //nolint:gosec // EVALSHA digest / config fingerprint, not security
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Gate tuning defaults.
const (
	defaultBurstSeconds       = 1.0
	defaultGlobalBurstCapMult = 1.20
	defaultIdleMs             = 10_000
	// defaultPriIdleMs is the cross-priority "demand-active" horizon: a peer
	// priority blocks our yield only when its bucket is full AND it last
	// granted within this window.
	defaultPriIdleMs = 2_000
)

// Environment variable names. Defaults match core/src/backpressure/queued_gate.py.
const (
	envBurstSeconds       = "QUEUED_GATE_BURST_SECONDS"
	envGlobalBurstCapMult = "QUEUED_GATE_GLOBAL_BURST_CAP_MULT"
	envRegistryIdleMs     = "QUEUED_GATE_REGISTRY_IDLE_MS"
	envPriIdleMs          = "QUEUED_GATE_PRI_IDLE_MS"
)

var defaultPriorityShares = map[int]float64{1: 0.5, 2: 0.3, 3: 0.2}

// ----- Public types -----------------------------------------------------------

// AcquireStats is the runtime diagnostic bundle returned by a successful
// QueuedAcquire.
type AcquireStats struct {
	// WaitMs is the wall time the caller blocked, from monotonic clock.
	WaitMs int
	// Polls is the number of Valkey EVALs (>=1).
	Polls int
	// EnqueuedAtMs is wall-clock UnixMilli at the moment QueuedAcquire was
	// first called.
	EnqueuedAtMs int64
	// GrantedAtMs is the Lua now_ms reported by the EVAL that granted.
	GrantedAtMs int64
	// DominantDim is the dominant dimension reported by the gate at grant
	// time. Empty for single-dimension targets (N=1), where the Lua script
	// returns no label.
	DominantDim string
}

// QueuedAcquireRequest is the input for one QueuedAcquire call.
type QueuedAcquireRequest struct {
	TenantID      string
	Priority      int
	ResourceCosts map[string]float64
	RequestID     string
	// TimeoutS bounds the blocking poll loop. Use math.Inf or a very large
	// value for "no timeout".
	TimeoutS float64
	// Blocking selects behaviour on deny:
	//   true  -> sleep retry_after_ms and retry until TimeoutS
	//   false -> return *DeniedError immediately
	Blocking bool
}

// DeniedError is returned by a non-blocking acquire when the gate could not
// debit capacity right now. RetryAfterMs is the Lua-provided retry hint.
type DeniedError struct {
	RetryAfterMs int
	// BindingLabel is the binding constraint reported by the gate on deny:
	// "<dim>:global" | "<dim>:pri" | "share" for multi-dim targets, empty
	// for single-dim.
	BindingLabel string
}

func (e *DeniedError) Error() string {
	return fmt.Sprintf("backpressure: admission denied; retry after %d ms", e.RetryAfterMs)
}

// TimeoutError is returned when a blocking acquire exceeds TimeoutS.
type TimeoutError struct {
	RequestID string
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("backpressure: queued acquire timed out for request %q", e.RequestID)
}

// PollCallback receives (eval_ms, granted, label) for every Valkey EVAL inside
// a QueuedAcquire poll loop. label is the dominant dim on grant or the binding
// label on deny (empty for single-dim targets).
type PollCallback func(evalMs int, granted bool, label string)

// ----- Key helpers ------------------------------------------------------------

// TBGlobalKey returns the Redis key holding the global token bucket (single-dim
// targets).
func TBGlobalKey(targetID string) string { return "bpq:" + targetID + ":tb:global" }

// TBPriKey returns the Redis key holding the per-priority buckets and
// last-active timestamps (single-dim targets).
func TBPriKey(targetID string) string { return "bpq:" + targetID + ":tb:pri" }

// TBGlobalKeyDim returns the per-dimension global bucket key used by the
// multi-dimension (N>=2) admission script. The :{dim} suffix keeps the multi-dim
// keyspace disjoint from the single-dim TBGlobalKey.
func TBGlobalKeyDim(targetID, dim string) string {
	return "bpq:" + targetID + ":tb:global:" + dim
}

// TBPriKeyDim returns the per-dimension per-priority bucket key used by the
// multi-dimension admission script.
func TBPriKeyDim(targetID, dim string) string {
	return "bpq:" + targetID + ":tb:pri:" + dim
}

// TBTPKey returns the Redis key holding the per-(tenant, priority) buckets.
func TBTPKey(targetID string) string { return "bpq:" + targetID + ":tb:tp" }

// TBRegKey returns the Redis key holding the tenant registry and sweep
// timestamps.
func TBRegKey(targetID string) string { return "bpq:" + targetID + ":tb:reg" }

// TBActKey returns the per-priority active-tenant sorted set (member=tenant,
// score=last_used_ms) used by the incremental fair-share denominator.
func TBActKey(targetID string, priority int) string {
	return "bpq:" + targetID + ":tb:act:" + strconv.Itoa(priority)
}

// TBAggKey returns the per-priority aggregate hash (sum_w, denom_d, cfg_ver,
// rebuild_ms) used by the incremental fair-share denominator.
func TBAggKey(targetID string, priority int) string {
	return "bpq:" + targetID + ":tb:agg:" + strconv.Itoa(priority)
}

// TBConKey returns the per-priority contribution hash (member=tenant, value=
// stored cost vector) used by the multi-dim incremental DRF denominator so a
// stale member's contribution can be subtracted without re-reading the shared
// tp hash. Unused by the
// single-dim script, whose denominator is weight-only.
func TBConKey(targetID string, priority int) string {
	return "bpq:" + targetID + ":tb:con:" + strconv.Itoa(priority)
}

// cfgVersion fingerprints the config inputs that determine a per-priority
// aggregate. When any change, the Lua triggers a full rebuild of sum_w/denom_d
// from the tp hash. The digest only has to be self-consistent within a worker
// process (a mismatch between workers forces a one-shot rebuild, which is
// always correct), so this uses Go's native float formatting.
func cfgVersion(parts ...string) string {
	sum := sha1.Sum([]byte(strings.Join(parts, "|"))) //nolint:gosec // cache key, not security
	return hex.EncodeToString(sum[:])
}

// ----- Config readers ---------------------------------------------------------

func envFloat(name string, def float64) float64 {
	if v := os.Getenv(name); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

// resolveCost returns the cost charged for one constraint: the explicit
// resource_costs[dim] when present, else the constraint's DefaultCost, else
// 1.0.
func resolveCost(c Constraint, resourceCosts map[string]float64) float64 {
	if v, ok := resourceCosts[c.Dimension]; ok {
		return v
	}
	if c.DefaultCost != nil {
		return *c.DefaultCost
	}
	return 1.0
}

// priorityShares returns the normalized (share1, share2, share3) tuple.
func priorityShares(target TargetConfig) (float64, float64, float64) {
	pm := target.PriorityMinimums
	if len(pm) == 0 {
		pm = defaultPriorityShares
	}
	s1 := float64Or(pm, 1, defaultPriorityShares[1])
	s2 := float64Or(pm, 2, defaultPriorityShares[2])
	s3 := float64Or(pm, 3, defaultPriorityShares[3])
	total := s1 + s2 + s3
	if total <= 0 {
		return 1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0
	}
	return s1 / total, s2 / total, s3 / total
}

func float64Or(m map[int]float64, k int, def float64) float64 {
	if v, ok := m[k]; ok {
		return v
	}
	return def
}

func tenantWeight(target TargetConfig, tenantID string) float64 {
	if len(target.TenantWeights) == 0 {
		return target.DefaultWeight()
	}
	if w, ok := target.TenantWeights[tenantID]; ok {
		return w
	}
	return target.DefaultWeight()
}

func tenantWeightsJSON(target TargetConfig) (string, error) {
	if len(target.TenantWeights) == 0 {
		return "{}", nil
	}
	// Canonical encoding: keys are tenant IDs as strings, values floats.
	b, err := json.Marshal(target.TenantWeights)
	if err != nil {
		return "", fmt.Errorf("backpressure: encoding tenant_weights: %w", err)
	}
	return string(b), nil
}

// ----- EVAL wrapper -----------------------------------------------------------

// evalResult is the (granted, second, label) tuple the admission script
// returns. Label is the optional 3rd element (dominant dim on grant, binding
// label on deny) emitted only by the multi-dim script, it is empty for
// single-dim targets.
type evalResult struct {
	Granted bool
	Second  int64
	Label   string
}

// evalAdmission builds KEYS/ARGV and invokes the admission Lua script,
// dispatching to the single-dim script for N=1 targets and the multi-dim DRF
// script for N>=2. The KEYS/ARGV layout is the one documented in the Lua
// script headers, workers sharing a target_id coordinate transparently.
func evalAdmission(
	ctx context.Context,
	client valkey.Client,
	target TargetConfig,
	op string,
	tenantID string,
	priority int,
	resourceCosts map[string]float64,
	nowMs int64,
	burstSeconds float64,
	globalBurstMult float64,
	idleMs int,
	priIdleMs int,
) (evalResult, error) {
	if len(target.Constraints) == 0 {
		return evalResult{}, errors.New("backpressure: queued acquire requires at least one target constraint")
	}

	s1, s2, s3 := priorityShares(target)
	myW := tenantWeight(target, tenantID)
	weightsJSON, err := tenantWeightsJSON(target)
	if err != nil {
		return evalResult{}, err
	}
	defaultW := target.DefaultWeight()

	var (
		keys []string
		argv []string
		lua  *valkey.Lua
	)

	if len(target.Constraints) == 1 {
		c := target.Constraints[0]
		cost := resolveCost(c, resourceCosts)
		keys = []string{
			TBGlobalKey(target.TargetID),
			TBPriKey(target.TargetID),
			TBTPKey(target.TargetID),
			TBRegKey(target.TargetID),
			TBActKey(target.TargetID, priority),
			TBAggKey(target.TargetID, priority),
		}
		cfgVer := cfgVersion(
			"1", weightsJSON, formatFloat(defaultW), formatFloat(c.Limit),
			formatFloat(s1), formatFloat(s2), formatFloat(s3),
			strconv.Itoa(idleMs), strconv.Itoa(priIdleMs),
		)
		argv = []string{
			op,
			tenantID,
			strconv.Itoa(priority),
			formatFloat(cost),
			strconv.FormatInt(nowMs, 10),
			formatFloat(c.Limit),
			formatFloat(burstSeconds),
			formatFloat(globalBurstMult),
			strconv.Itoa(idleMs),
			formatFloat(s1),
			formatFloat(s2),
			formatFloat(s3),
			formatFloat(myW),
			weightsJSON,
			formatFloat(defaultW),
			strconv.Itoa(priIdleMs),
			cfgVer,
		}
		lua = admissionScript()
	} else {
		// Multi-dimension path (N >= 2).
		// KEYS: [reg, tp, (global:dim, pri:dim)*N, act:{pri}, agg:{pri}, con:{pri}].
		keys = make([]string, 0, 2+2*len(target.Constraints)+3)
		keys = append(keys, TBRegKey(target.TargetID), TBTPKey(target.TargetID))
		for _, c := range target.Constraints {
			keys = append(keys,
				TBGlobalKeyDim(target.TargetID, c.Dimension),
				TBPriKeyDim(target.TargetID, c.Dimension),
			)
		}
		keys = append(keys,
			TBActKey(target.TargetID, priority),
			TBAggKey(target.TargetID, priority),
			TBConKey(target.TargetID, priority),
		)
		argv = []string{
			op,
			tenantID,
			strconv.Itoa(priority),
			strconv.FormatInt(nowMs, 10),
			strconv.Itoa(len(target.Constraints)),
			formatFloat(myW),
			weightsJSON,
			formatFloat(defaultW),
			strconv.Itoa(idleMs),
			strconv.Itoa(priIdleMs),
			formatFloat(burstSeconds),
			formatFloat(globalBurstMult),
		}
		// cfg_ver fingerprints every aggregate input, a mismatch forces a full
		// rebuild from the tp hash. Self-consistent within a stack (see cfgVersion).
		cfgParts := []string{
			strconv.Itoa(len(target.Constraints)), weightsJSON, formatFloat(defaultW),
			strconv.Itoa(idleMs), strconv.Itoa(priIdleMs),
		}
		for _, c := range target.Constraints {
			cost := resolveCost(c, resourceCosts)
			cfgParts = append(cfgParts, c.Dimension, formatFloat(c.Limit),
				formatFloat(s1), formatFloat(s2), formatFloat(s3))
			argv = append(argv,
				c.Dimension,
				formatFloat(c.Limit),
				formatFloat(cost),
				formatFloat(s1),
				formatFloat(s2),
				formatFloat(s3),
			)
		}
		argv = append(argv, cfgVersion(cfgParts...))
		lua = multiAdmissionScript()
	}

	resp := lua.Exec(ctx, client, keys, argv)
	if err := resp.Error(); err != nil {
		return evalResult{}, fmt.Errorf("backpressure: EVAL admission script: %w", err)
	}
	arr, err := resp.ToArray()
	if err != nil {
		return evalResult{}, fmt.Errorf("backpressure: admission script returned non-array: %w", err)
	}
	if len(arr) < 1 {
		return evalResult{}, fmt.Errorf("backpressure: admission script returned %d elements", len(arr))
	}
	st, err := arr[0].AsInt64()
	if err != nil {
		return evalResult{}, fmt.Errorf("backpressure: admission script status field: %w", err)
	}
	res := evalResult{Granted: st == 1}
	if len(arr) >= 2 {
		second, err := arr[1].AsInt64()
		if err != nil {
			return evalResult{}, fmt.Errorf("backpressure: admission script second field: %w", err)
		}
		res.Second = second
	}
	if len(arr) >= 3 {
		// Optional label: dominant dim on grant / binding label on deny.
		// Only the multi-dim script emits it, ignore decode errors.
		if label, lerr := arr[2].ToString(); lerr == nil {
			res.Label = label
		}
	}
	return res, nil
}

// formatFloat renders a float64 as a non-exponential, full-precision
// representation that the Lua tonumber() parser handles. valkey-go forwards arguments as RESP bulk strings, so
// any human-readable form works.
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// ----- Public API: QueuedAcquire / QueuedRelease ------------------------------

// QueuedAcquire blocks until admission is granted, until the request's
// TimeoutS elapses, or until ctx is cancelled.
//
// On grant returns nil and an AcquireStats describing the wait.
// On non-blocking deny returns *DeniedError.
// On timeout returns *TimeoutError.
// On Valkey transport / parse failure returns an unwrapped error.
//
// Enforces all dimensions in target.Constraints atomically: N=1 uses the
// single-resource script, N>=2 the multi-dim DRF script.
//
// recordPoll, if non-nil, is invoked once per EVAL with (eval_ms, granted,
// label) where label is the dominant dim on grant or the binding label on
// deny (empty for single-dim targets).
func QueuedAcquire(
	ctx context.Context,
	client valkey.Client,
	target TargetConfig,
	req QueuedAcquireRequest,
	recordPoll PollCallback,
) (AcquireStats, error) {
	if len(target.Constraints) == 0 {
		return AcquireStats{}, errors.New("backpressure: target has no constraints")
	}

	burstSeconds := envFloat(envBurstSeconds, defaultBurstSeconds)
	gMult := envFloat(envGlobalBurstCapMult, defaultGlobalBurstCapMult)
	idleMs := envInt(envRegistryIdleMs, defaultIdleMs)
	priIdleMs := envInt(envPriIdleMs, defaultPriIdleMs)

	start := time.Now()
	startMs := start.UnixMilli()
	polls := 0

	for {
		polls++
		if err := ctx.Err(); err != nil {
			return AcquireStats{}, err
		}
		nowMs := time.Now().UnixMilli()
		tEval := time.Now()
		res, err := evalAdmission(
			ctx, client, target,
			"acquire", req.TenantID, req.Priority, req.ResourceCosts, nowMs,
			burstSeconds, gMult, idleMs, priIdleMs,
		)
		evalMs := int(time.Since(tEval) / time.Millisecond)
		if err != nil {
			return AcquireStats{}, err
		}
		if recordPoll != nil {
			// A misbehaving callback must not
			// break the loop.
			func() {
				defer func() { _ = recover() }()
				recordPoll(evalMs, res.Granted, res.Label)
			}()
		}
		if res.Granted {
			return AcquireStats{
				WaitMs:       int(time.Since(start) / time.Millisecond),
				Polls:        polls,
				EnqueuedAtMs: startMs,
				GrantedAtMs:  res.Second,
				DominantDim:  res.Label,
			}, nil
		}

		retryAfterMs := int(res.Second)
		if retryAfterMs < 1 {
			retryAfterMs = 1
		}
		if !req.Blocking {
			return AcquireStats{}, &DeniedError{RetryAfterMs: retryAfterMs, BindingLabel: res.Label}
		}
		if time.Since(start).Seconds() >= req.TimeoutS {
			return AcquireStats{}, &TimeoutError{RequestID: req.RequestID}
		}

		// Sleep retry_after_ms but interruptible by ctx and capped by the
		// remaining timeout so we don't overshoot it by ~retry_after_ms.
		remaining := req.TimeoutS - time.Since(start).Seconds()
		sleep := time.Duration(retryAfterMs) * time.Millisecond
		if remaining > 0 && time.Duration(remaining*float64(time.Second)) < sleep {
			sleep = time.Duration(remaining * float64(time.Second))
		}
		if sleep < time.Millisecond {
			sleep = time.Millisecond
		}
		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
			return AcquireStats{}, ctx.Err()
		case <-t.C:
		}
	}
}

// QueuedRelease refunds capacity reserved by a successful grant when upstream
// work did not consume it. Best-effort single round-trip. Errors are
// returned so callers can decide whether to swallow them.
func QueuedRelease(
	ctx context.Context,
	client valkey.Client,
	target TargetConfig,
	tenantID string,
	resourceCosts map[string]float64,
	priority int,
) error {
	if len(target.Constraints) == 0 {
		return errors.New("backpressure: target has no constraints")
	}

	burstSeconds := envFloat(envBurstSeconds, defaultBurstSeconds)
	gMult := envFloat(envGlobalBurstCapMult, defaultGlobalBurstCapMult)
	idleMs := envInt(envRegistryIdleMs, defaultIdleMs)
	priIdleMs := envInt(envPriIdleMs, defaultPriIdleMs)

	// Always EVAL: a zero or negative cost
	// is forwarded so the multi-dim reconciliation "debit" path (negative
	// resource_costs) reaches the Lua release op.
	_, err := evalAdmission(
		ctx, client, target,
		"release", tenantID, priority, resourceCosts, time.Now().UnixMilli(),
		burstSeconds, gMult, idleMs, priIdleMs,
	)
	return err
}

// IsDenied reports whether err is a *DeniedError and returns the
// retry-after hint when so.
func IsDenied(err error) (int, bool) {
	var de *DeniedError
	if errors.As(err, &de) {
		return de.RetryAfterMs, true
	}
	return 0, false
}

// IsTimeout reports whether err is a *TimeoutError.
func IsTimeout(err error) bool {
	var te *TimeoutError
	return errors.As(err, &te)
}
