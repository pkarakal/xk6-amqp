package amqp10_test

import (
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.k6.io/k6/js/modulestest"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/lib/netext"
	"go.k6.io/k6/lib/types"
	"go.k6.io/k6/metrics"
)

// newVUState creates a minimal lib.State that satisfies the connect() guard
// (vu.State() != nil) and moves the runtime into VU context.
func newVUState(t testing.TB, testRT *modulestest.Runtime) *lib.State {
	t.Helper()
	registry := metrics.NewRegistry()
	state := &lib.State{
		Logger:         logrus.New(),
		BufferPool:     lib.NewBufferPool(),
		Samples:        make(chan metrics.SampleContainer, 100),
		Tags:           lib.NewVUStateTags(registry.RootTagSet()),
		BuiltinMetrics: metrics.RegisterBuiltinMetrics(registry),
		Dialer: netext.NewDialer(
			net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 10 * time.Second,
			},
			netext.NewResolver(net.LookupIP, 0, types.DNSfirst, types.DNSpreferIPv4),
		),
	}
	testRT.MoveToVUContext(state)
	return state
}
