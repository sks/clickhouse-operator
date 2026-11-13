package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// TestIdentityPersistRoundTrip: the persist closure must write the
// settled identity into the state file (the job subcommands' contract).
func TestIdentityPersistRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := NewFileStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(nil, store)
	persist := engine.IdentityPersister("aws")

	id := model.NodeIdentity{}
	id.Hostname = "writer-0"
	id.CRName = "01-0"
	persist(id)

	data, _ := os.ReadFile(path)
	var st map[string]any
	if err := json.Unmarshal(data, &st); err != nil {
		t.Fatal(err)
	}
	identity, ok := st["identity"].(map[string]any)
	if !ok {
		t.Fatalf("identity missing after persist: %s", data)
	}
	if identity["hostname"] != "writer-0" {
		t.Fatalf("wrong identity: %v", identity)
	}
	t.Log("identity persisted OK")
}

// TestIdentitySurvivesStepRecords: the run's per-step Saves must
// PRESERVE the identity the persister wrote mid-run (the clobber bug:
// Run loaded state before settle, then overwrote the identity).
func TestIdentitySurvivesStepRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := NewFileStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(nil, store).WithIdentity(nil, "aws", "hash-1")
	persist := engine.IdentityPersister("aws")

	// Run's start-of-run load (empty file → fresh state)
	run := func() {
		var state *modelState
		if loaded, err := store.Load(); err == nil && loaded != nil {
			state = loaded
		}
		if state == nil {
			state = &modelState{}
		}
		engine.shared = state
		// a step records and saves
		state.recordStep(stepRecord{Name: "fetch", Status: "done"})
		_ = store.Save(state)
	}
	run()
	persist(model.NodeIdentity{Hostname: "writer-0"})
	run() // second run-like pass: must not lose identity

	data, _ := os.ReadFile(path)
	var st modelState
	if err := json.Unmarshal(data, &st); err != nil {
		t.Fatal(err)
	}
	if st.Identity == nil || st.Identity.Hostname != "writer-0" {
		t.Fatalf("identity clobbered by step records: %+v", st.Identity)
	}
	t.Log("identity survived step records")
}
