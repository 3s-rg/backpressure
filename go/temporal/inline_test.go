package bptemporal

import (
	"context"
	"errors"
	"testing"
)

// These tests exercise the reflection-based inline dispatch in isolation
// from Valkey / Temporal so we can verify the function-signature handling
// quickly.

func TestInvokeInline_ContextAndArgs(t *testing.T) {
	called := false
	fn := func(ctx context.Context, name string, n int) (map[string]any, error) {
		if ctx == nil {
			t.Errorf("ctx is nil")
		}
		called = true
		return map[string]any{"name": name, "n": n}, nil
	}
	out, err := invokeInline(context.Background(), fn, []any{"alice", 42})
	if err != nil {
		t.Fatalf("invokeInline: %v", err)
	}
	if !called {
		t.Fatal("fn not called")
	}
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result %T not map", out)
	}
	if m["name"] != "alice" {
		t.Errorf("name=%v", m["name"])
	}
}

func TestInvokeInline_NoContext(t *testing.T) {
	fn := func(name string) (string, error) { return "hello " + name, nil }
	out, err := invokeInline(context.Background(), fn, []any{"bob"})
	if err != nil {
		t.Fatalf("invokeInline: %v", err)
	}
	if out != "hello bob" {
		t.Errorf("got %v", out)
	}
}

func TestInvokeInline_ErrorReturn(t *testing.T) {
	sentinel := errors.New("boom")
	fn := func(ctx context.Context) error { return sentinel }
	_, err := invokeInline(context.Background(), fn, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
}

func TestInvokeInline_WrongArity(t *testing.T) {
	fn := func(ctx context.Context, a int, b int) (int, error) { return a + b, nil }
	_, err := invokeInline(context.Background(), fn, []any{1})
	if err == nil {
		t.Fatal("expected arity mismatch error")
	}
}

func TestInvokeInline_NumericCoercion(t *testing.T) {
	// JSON decodes numbers into float64, ensure they reach an int parameter.
	fn := func(ctx context.Context, n int) (int, error) { return n * 2, nil }
	out, err := invokeInline(context.Background(), fn, []any{float64(21)})
	if err != nil {
		t.Fatalf("invokeInline: %v", err)
	}
	if out != 42 {
		t.Errorf("got %v", out)
	}
}
