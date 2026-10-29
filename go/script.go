package backpressure

import (
	_ "embed"
	"sync"

	"github.com/valkey-io/valkey-go"
)

//go:embed lua/admission_token_buckets.lua
var admissionScriptSource string

//go:embed lua/admission_token_buckets_multi.lua
var multiAdmissionScriptSource string

var (
	scriptOnce sync.Once
	script     *valkey.Lua

	multiScriptOnce sync.Once
	multiScript     *valkey.Lua
)

// admissionScript returns the singleton Lua handle for the single-dimension
// admission script. valkey-go's *Lua sends EVALSHA with a transparent EVAL
// fallback on NOSCRIPT.
func admissionScript() *valkey.Lua {
	scriptOnce.Do(func() {
		script = valkey.NewLuaScript(admissionScriptSource)
	})
	return script
}

// multiAdmissionScript returns the singleton Lua handle for the
// multi-dimension (N>=2) DRF admission script. Workers sharing a target_id
// coordinate through it regardless of language.
func multiAdmissionScript() *valkey.Lua {
	multiScriptOnce.Do(func() {
		multiScript = valkey.NewLuaScript(multiAdmissionScriptSource)
	})
	return multiScript
}

// AdmissionScriptSource exposes the embedded single-dim Lua text for debugging
// or sharing.
func AdmissionScriptSource() string {
	return admissionScriptSource
}

// MultiAdmissionScriptSource exposes the embedded multi-dim Lua text for
// debugging or sharing.
func MultiAdmissionScriptSource() string {
	return multiAdmissionScriptSource
}
