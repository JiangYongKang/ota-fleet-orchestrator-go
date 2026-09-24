package rollout

import (
	"testing"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

type testEnv struct {
	o   *Orchestrator
	clk *testClock
	sim *SimDispatcher
}

type testClock struct{ t time.Time }

func (c *testClock) now() time.Time                { return c.t }
func (c *testClock) add(d time.Duration) time.Time { c.t = c.t.Add(d); return c.t }

func mustVer(s string) version.Version {
	v, err := version.Parse(s)
	if err != nil {
		panic(err)
	}
	return v
}

// newEnv 构造基于临时文件、可控时钟的编排器。
func newEnv(t *testing.T, batchSize int) testEnv {
	t.Helper()
	clk := &testClock{t: time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)}
	o, err := New(Config{
		OfflineTimeout: 30 * time.Second,
		JobTimeout:     60 * time.Second,
		BatchSize:      batchSize,
		TickInterval:   time.Second,
		StorePath:      t.TempDir() + "/state.json",
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	sim, _ := o.SimDispatcher()
	return testEnv{o: o, clk: clk, sim: sim}
}

func (e testEnv) register(id, group, fw string) {
	if err := e.o.RegisterDevice(device.Device{ID: id, Group: group, CurrentVersion: mustVer(fw)}); err != nil {
		panic(err)
	}
}

func (e testEnv) beat(id, fw string, seq int64) {
	if _, err := e.o.Heartbeat(id, mustVer(fw), seq); err != nil {
		panic(err)
	}
}

func (e testEnv) state(id string) string {
	j, ok := e.o.Job(id)
	if !ok {
		return "<no-job>"
	}
	return j.State
}
