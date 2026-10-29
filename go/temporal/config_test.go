package bptemporal_test

import (
	"os"
	"path/filepath"
	"testing"

	bptemporal "github.com/stano45/backpressure/go/temporal"
)

func TestLoadConfigYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	yaml := `
targets:
  llm-api:
    constraints:
      - dimension: rps
        limit: 100
    tenant_weights:
      team_a: 1.0
      team_b: 2.0
    priority_minimums:
      "1": 0.5
      "2": 0.3
      "3": 0.2
activity_targets:
  CallLLM: llm-api
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	targets, actTargets, err := bptemporal.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	tc, ok := targets["llm-api"]
	if !ok {
		t.Fatalf("targets[llm-api] missing; got %+v", targets)
	}
	if tc.TargetID != "llm-api" {
		t.Errorf("target_id = %q, want llm-api", tc.TargetID)
	}
	if len(tc.Constraints) != 1 || tc.Constraints[0].Dimension != "rps" || tc.Constraints[0].Limit != 100 {
		t.Errorf("constraints = %+v", tc.Constraints)
	}
	if tc.TenantWeights["team_a"] != 1.0 || tc.TenantWeights["team_b"] != 2.0 {
		t.Errorf("tenant_weights = %+v", tc.TenantWeights)
	}
	if tc.PriorityMinimums[1] != 0.5 || tc.PriorityMinimums[2] != 0.3 || tc.PriorityMinimums[3] != 0.2 {
		t.Errorf("priority_minimums = %+v", tc.PriorityMinimums)
	}
	if actTargets["CallLLM"] != "llm-api" {
		t.Errorf("activity_targets[CallLLM] = %q", actTargets["CallLLM"])
	}
}

func TestLoadConfigJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.json")
	body := `{
  "targets": {
    "llm-api": {
      "constraints": [{"dimension": "rps", "limit": 100}]
    }
  },
  "activity_targets": {"CallLLM": "llm-api"}
}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	targets, _, err := bptemporal.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if _, ok := targets["llm-api"]; !ok {
		t.Fatalf("missing llm-api: %+v", targets)
	}
}
