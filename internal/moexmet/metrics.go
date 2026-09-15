/*
FILE: internal/moexmet/metrics.go

DESCRIPTION:
Minimal counter and counter-factory interface for the SDK. Shaped like
prometheus.Counter (Inc/Add) but not tied to Prometheus — the user attaches
any backend via a thin adapter. Placed in internal so that fix/simba/iss can
use it without importing the root package.

COUNTER NAMING IN THE SDK (see docs for full list):

	moex_fix_reconnects_total
	moex_fix_messages_sent_total
	moex_fix_messages_received_total
	moex_simba_packets_received_total
	moex_simba_packets_dropped_total
	moex_simba_gap_detected_total
	moex_iss_requests_total
	moex_twime_messages_sent_total{template}
	moex_twime_messages_received_total{template}
	moex_twime_heartbeats_sent_total
	moex_twime_flood_rejects_total
	moex_twime_session_rejects_total{reason}
	moex_twime_business_rejects_total
	moex_twime_retransmit_requests_total
	moex_twime_retransmitted_messages_total
	moex_twime_seq_mismatch_total
	moex_twime_terminates_total{code}
	moex_twime_pacer_waits_total
	moex_twime_establish_rejects_total{code}
*/
package moexmet

// Counter — a single counter (monotonically increasing number).
type Counter interface {
	// Inc increments the value by 1.
	Inc()
	// Add increments the value by delta. If delta < 0, the implementation may
	// ignore it (counter is monotonic).
	Add(delta float64)
}

// CounterFactory — counter factory. labels — k1, v1, k2, v2, ... .
type CounterFactory interface {
	// Counter returns a counter by name and labels. The implementation must
	// guarantee that the same (name, labels) set always returns the same
	// entry (e.g. via sync.Map).
	Counter(name string, labels ...string) Counter
}

// Noop — default implementation.
type noopFactory struct{}
type noopCounter struct{}

// Noop returns a no-op factory (singleton).
func Noop() CounterFactory { return noopFactorySingleton }

var (
	noopFactorySingleton CounterFactory = noopFactory{}
	noopCounterSingleton Counter        = noopCounter{}
)

func (noopFactory) Counter(string, ...string) Counter { return noopCounterSingleton }

func (noopCounter) Inc()        {}
func (noopCounter) Add(float64) {}
