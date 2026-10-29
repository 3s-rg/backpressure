package bptemporal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/valkey-io/valkey-go"
	"gopkg.in/yaml.v3"

	bp "github.com/stano45/backpressure/go"
)

// Config is the on-disk shape that LoadConfig reads. The same YAML/JSON
// config file works for Python and Go workers.
//
//	targets:
//	  llm-api:
//	    target_id: llm-api            # optional, key is used when absent
//	    constraints:
//	      - { dimension: rps, limit: 100 }
//	    tenant_weights: { team_a: 1.0 }
//	    priority_minimums: { "1": 0.5, "2": 0.3, "3": 0.2 }
//	activity_targets:
//	  call_llm: llm-api
type Config struct {
	Targets         map[string]map[string]any `yaml:"targets"         json:"targets"`
	ActivityTargets map[string]string         `yaml:"activity_targets" json:"activity_targets"`
}

// LoadConfig reads a YAML (.yaml/.yml) or JSON config file and returns a
// resolved (targets, activity_targets) pair suitable for GateOptions.
func LoadConfig(path string) (map[string]bp.TargetConfig, map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("backpressure: read config %s: %w", path, err)
	}

	var cfg Config
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(raw, &cfg); err != nil {
			return nil, nil, fmt.Errorf("backpressure: parse YAML %s: %w", path, err)
		}
	case ".json":
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, nil, fmt.Errorf("backpressure: parse JSON %s: %w", path, err)
		}
	default:
		// Best-effort: try YAML first (YAML is a JSON superset for simple cases),
		// fall back to JSON if that fails.
		if yerr := yaml.Unmarshal(raw, &cfg); yerr != nil {
			if jerr := json.Unmarshal(raw, &cfg); jerr != nil {
				return nil, nil, fmt.Errorf(
					"backpressure: parse %s as YAML (%v) or JSON (%v)", path, yerr, jerr,
				)
			}
		}
	}

	targets := make(map[string]bp.TargetConfig, len(cfg.Targets))
	for key, m := range cfg.Targets {
		if m == nil {
			m = map[string]any{}
		}
		if _, ok := m["target_id"]; !ok {
			m["target_id"] = key
		}
		// yaml.v3 decodes maps as map[string]any with nested ints/floats, we
		// have already normalized the top-level into map[string]any, but the
		// nested constraints + priority_minimums use generic shapes that
		// TargetConfigFromMap needs. Pre-coerce nested maps that the YAML
		// decoder might have produced as map[interface{}]interface{}.
		m = normalizeMap(m)
		tc, err := bp.TargetConfigFromMap(m)
		if err != nil {
			return nil, nil, fmt.Errorf("backpressure: target %q: %w", key, err)
		}
		targets[tc.TargetID] = tc
	}

	return targets, cfg.ActivityTargets, nil
}

// NewGateFromConfig is a convenience wrapper combining LoadConfig with
// NewGate. inlineActivities is optional and only used in inline gate mode.
func NewGateFromConfig(
	client valkey.Client,
	path string,
	inlineActivities map[string]any,
) (*Gate, error) {
	targets, actTargets, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return NewGate(client, GateOptions{
		Targets:          targets,
		ActivityTargets:  actTargets,
		InlineActivities: inlineActivities,
	}), nil
}

// normalizeMap converts any nested map[interface{}]interface{} (produced by
// some YAML libraries) into map[string]any, recursively. yaml.v3 decodes
// JSON-style mappings directly into map[string]any so this is usually a
// no-op, but a defensive coerce keeps the FromMap parser happy regardless.
func normalizeMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = normalize(v)
	}
	return out
}

func normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return normalizeMap(x)
	case map[any]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[fmt.Sprintf("%v", k)] = normalize(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = normalize(item)
		}
		return out
	default:
		return v
	}
}
