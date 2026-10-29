// Package backpressure is a Valkey-backed queued admission gate that ports the
// Python `backpressure` library to Go.
//
// The admission decision (global + per-priority + per-tenant token buckets
// with weighted fair share and cross-priority yield) lives in the Lua script
// embedded by script.go. Go workers and Python workers that share the same
// Valkey instance and target_id transparently interoperate.
package backpressure

import (
	"errors"
	"fmt"
)

// Constraint is a single rate-limit rule for one resource dimension.
//
// QueuedAcquire / QueuedRelease enforce ALL constraints in
// TargetConfig.Constraints atomically: a target with one constraint uses the
// single-dimension admission script, and a target with two or more uses the
// multi-dimension DRF script. Per-request cost for each dimension comes from
// resource_costs[dimension] or DefaultCost.
type Constraint struct {
	// Dimension name (e.g. "rpm", "tokens").
	Dimension string
	// Limit is the global refill rate in units per second, must be > 0.
	Limit float64
	// DefaultCost is the cost debited when a request omits this dimension.
	// nil means "fall back to 1.0 at acquire time".
	DefaultCost *float64
}

// Validate returns an error if the constraint is malformed.
func (c Constraint) Validate() error {
	if c.Dimension == "" {
		return errors.New("constraint: dimension is required")
	}
	if !(c.Limit > 0) {
		return fmt.Errorf("constraint: limit must be positive, got %v", c.Limit)
	}
	return nil
}

// TargetConfig is the full configuration for a rate-limited upstream target.
//
// A target identifies a single upstream service or API surface
// (e.g. "openai-gpt4"). All tenants competing for the same upstream share
// the same TargetConfig and the same Valkey key prefix
// "bpq:{TargetID}:tb:*".
type TargetConfig struct {
	// TargetID is the Valkey key prefix discriminator. Workers in different
	// processes that share the same TargetID share the same buckets.
	TargetID string

	// Constraints carries at least one entry, ALL entries are enforced
	// atomically at runtime. N=1 uses the
	// single-dim script, N>=2 the multi-dim DRF script. The order is
	// significant for multi-dim targets: it must match across any Go/Python
	// workers sharing the target (the per-tenant cost vector is positional).
	Constraints []Constraint

	// TenantWeights is the per-tenant weight for weighted fair-share
	// allocation within each priority class. Tenants absent from this map
	// receive DefaultWeight() at acquire time.
	TenantWeights map[string]float64

	// PriorityMinimums is the per-priority refill share fraction for
	// priorities 1..3. Values are normalized to sum to 1 at runtime.
	// Defaults to {1: 0.5, 2: 0.3, 3: 0.2} when empty.
	PriorityMinimums map[int]float64
}

// DefaultWeight returns the minimum configured tenant weight, or 1.0 when
// no weights are set.
func (t TargetConfig) DefaultWeight() float64 {
	if len(t.TenantWeights) == 0 {
		return 1.0
	}
	min := 0.0
	first := true
	for _, w := range t.TenantWeights {
		if first || w < min {
			min = w
			first = false
		}
	}
	return min
}

// Validate ensures the target has the minimum information QueuedAcquire needs.
func (t TargetConfig) Validate() error {
	if t.TargetID == "" {
		return errors.New("TargetConfig: target_id is required")
	}
	if len(t.Constraints) == 0 {
		return errors.New("TargetConfig: at least one constraint is required")
	}
	for i, c := range t.Constraints {
		if err := c.Validate(); err != nil {
			return fmt.Errorf("TargetConfig.Constraints[%d]: %w", i, err)
		}
	}
	for w := range t.TenantWeights {
		if !(t.TenantWeights[w] > 0) {
			return fmt.Errorf("TargetConfig.TenantWeights[%q] must be positive", w)
		}
	}
	for p, v := range t.PriorityMinimums {
		if p < 1 || p > 3 {
			return fmt.Errorf("TargetConfig.PriorityMinimums: priority %d out of range [1,3]", p)
		}
		if v < 0 {
			return fmt.Errorf("TargetConfig.PriorityMinimums[%d]: must be >= 0, got %v", p, v)
		}
	}
	return nil
}

// ToMap returns a JSON/YAML-friendly representation, so configs can be
// round-tripped between languages.
func (t TargetConfig) ToMap() map[string]any {
	constraints := make([]map[string]any, 0, len(t.Constraints))
	for _, c := range t.Constraints {
		m := map[string]any{
			"dimension": c.Dimension,
			"limit":     c.Limit,
		}
		if c.DefaultCost != nil {
			m["default_cost"] = *c.DefaultCost
		}
		constraints = append(constraints, m)
	}
	weights := make(map[string]float64, len(t.TenantWeights))
	for k, v := range t.TenantWeights {
		weights[k] = v
	}
	prios := make(map[string]float64, len(t.PriorityMinimums))
	for k, v := range t.PriorityMinimums {
		prios[fmt.Sprintf("%d", k)] = v
	}
	return map[string]any{
		"target_id":         t.TargetID,
		"constraints":       constraints,
		"tenant_weights":    weights,
		"priority_minimums": prios,
	}
}

// TargetConfigFromMap reconstructs a TargetConfig from the wire shape
// produced by ToMap.
func TargetConfigFromMap(m map[string]any) (TargetConfig, error) {
	tc := TargetConfig{
		TenantWeights:    map[string]float64{},
		PriorityMinimums: map[int]float64{},
	}
	tid, ok := m["target_id"].(string)
	if !ok || tid == "" {
		return tc, errors.New("TargetConfigFromMap: target_id is required")
	}
	tc.TargetID = tid

	rawConstraints, _ := m["constraints"].([]any)
	for i, raw := range rawConstraints {
		cm, ok := raw.(map[string]any)
		if !ok {
			return tc, fmt.Errorf("TargetConfigFromMap: constraints[%d]: not a map", i)
		}
		dim, _ := cm["dimension"].(string)
		limit, err := asFloat64(cm["limit"])
		if err != nil {
			return tc, fmt.Errorf("TargetConfigFromMap: constraints[%d].limit: %w", i, err)
		}
		c := Constraint{Dimension: dim, Limit: limit}
		if dc, ok := cm["default_cost"]; ok && dc != nil {
			v, err := asFloat64(dc)
			if err != nil {
				return tc, fmt.Errorf("TargetConfigFromMap: constraints[%d].default_cost: %w", i, err)
			}
			c.DefaultCost = &v
		}
		tc.Constraints = append(tc.Constraints, c)
	}

	if rawWeights, ok := m["tenant_weights"].(map[string]any); ok {
		for k, v := range rawWeights {
			fv, err := asFloat64(v)
			if err != nil {
				return tc, fmt.Errorf("TargetConfigFromMap: tenant_weights[%q]: %w", k, err)
			}
			tc.TenantWeights[k] = fv
		}
	}

	if rawPrios, ok := m["priority_minimums"].(map[string]any); ok {
		for k, v := range rawPrios {
			var pi int
			if _, err := fmt.Sscanf(k, "%d", &pi); err != nil {
				return tc, fmt.Errorf("TargetConfigFromMap: priority_minimums key %q: %w", k, err)
			}
			fv, err := asFloat64(v)
			if err != nil {
				return tc, fmt.Errorf("TargetConfigFromMap: priority_minimums[%q]: %w", k, err)
			}
			tc.PriorityMinimums[pi] = fv
		}
	}

	if err := tc.Validate(); err != nil {
		return tc, err
	}
	return tc, nil
}

func asFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint:
		return float64(x), nil
	case uint32:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case string:
		var f float64
		if _, err := fmt.Sscanf(x, "%g", &f); err != nil {
			return 0, fmt.Errorf("cannot parse %q as float64: %w", x, err)
		}
		return f, nil
	case nil:
		return 0, errors.New("nil value")
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", v)
	}
}
