/*
FILE: internal/twime/metrics.go

DESCRIPTION:
Counters of the TWIME session, pre-created at construction so the hot
path does a slice index instead of a (name, labels) lookup per message.
Names follow the SDK convention (internal/moexmet):

	moex_twime_messages_sent_total{template}
	moex_twime_messages_received_total{template}
	moex_twime_heartbeats_sent_total
	moex_twime_flood_rejects_total
	moex_twime_session_rejects_total{reason}
	moex_twime_business_rejects_total
	moex_twime_retransmit_requests_total
	moex_twime_retransmitted_messages_total
	moex_twime_seq_mismatch_total
	moex_twime_terminates_total{code}       — server-initiated only
	moex_twime_pacer_waits_total
	moex_twime_establish_rejects_total{code}

The template label is the schema message name (28 values), never an id.
*/
package twime

import (
	"strconv"

	"github.com/tonymontanov/go-moex/internal/moexmet"
)

var knownTemplates = []uint16{
	TemplateEstablish, TemplateEstablishmentAck, TemplateEstablishmentReject, TemplateTerminate,
	TemplateRetransmitRequest, TemplateRetransmission, TemplateSequence, TemplateFloodReject,
	TemplateSessionReject, TemplateBusinessMessageReject,
	TemplateNewOrderSingle, TemplateOrderMassCancelRequest, TemplateOrderMassCancelByBFLimitRequest,
	TemplateOrderCancelRequest, TemplateOrderReplaceRequest, TemplateNewOrderIceberg,
	TemplateOrderIcebergCancelRequest, TemplateOrderIcebergReplaceRequest, TemplateNewOrderIcebergX,
	TemplateOrderMassCancelResponse, TemplateEmptyBook, TemplateSystemEvent, TemplateNewOrderSingleResponse,
	TemplateNewOrderIcebergResponse, TemplateOrderCancelResponse, TemplateOrderReplaceResponse,
	TemplateExecutionSingleReport, TemplateExecutionMultilegReport,
}

type metrics struct {
	sent          map[uint16]moexmet.Counter
	received      map[uint16]moexmet.Counter
	sentUnknown   moexmet.Counter
	recvUnknown   moexmet.Counter
	heartbeats    moexmet.Counter
	floodRejects  moexmet.Counter
	sessionRej    map[SessionRejectReason]moexmet.Counter
	sessionRejX   moexmet.Counter
	businessRej   moexmet.Counter
	retransmitRq  moexmet.Counter
	retransmitted moexmet.Counter
	seqMismatch   moexmet.Counter
	terminates    map[TerminationCode]moexmet.Counter
	terminatesX   moexmet.Counter
	pacerWaits    moexmet.Counter
	establishRej  map[EstablishmentRejectCode]moexmet.Counter
	establishRejX moexmet.Counter
}

func newMetrics(f moexmet.CounterFactory) *metrics {
	if f == nil {
		f = moexmet.Noop()
	}
	m := &metrics{
		sent:          make(map[uint16]moexmet.Counter, len(knownTemplates)),
		received:      make(map[uint16]moexmet.Counter, len(knownTemplates)),
		sentUnknown:   f.Counter("moex_twime_messages_sent_total", "template", "unknown"),
		recvUnknown:   f.Counter("moex_twime_messages_received_total", "template", "unknown"),
		heartbeats:    f.Counter("moex_twime_heartbeats_sent_total"),
		floodRejects:  f.Counter("moex_twime_flood_rejects_total"),
		sessionRej:    make(map[SessionRejectReason]moexmet.Counter, 4),
		sessionRejX:   f.Counter("moex_twime_session_rejects_total", "reason", "unknown"),
		businessRej:   f.Counter("moex_twime_business_rejects_total"),
		retransmitRq:  f.Counter("moex_twime_retransmit_requests_total"),
		retransmitted: f.Counter("moex_twime_retransmitted_messages_total"),
		seqMismatch:   f.Counter("moex_twime_seq_mismatch_total"),
		terminates:    make(map[TerminationCode]moexmet.Counter, 12),
		terminatesX:   f.Counter("moex_twime_terminates_total", "code", "unknown"),
		pacerWaits:    f.Counter("moex_twime_pacer_waits_total"),
		establishRej:  make(map[EstablishmentRejectCode]moexmet.Counter, 7),
		establishRejX: f.Counter("moex_twime_establish_rejects_total", "code", "unknown"),
	}
	for _, id := range knownTemplates {
		m.sent[id] = f.Counter("moex_twime_messages_sent_total", "template", TemplateName(id))
		m.received[id] = f.Counter("moex_twime_messages_received_total", "template", TemplateName(id))
	}
	for _, r := range []SessionRejectReason{SessionRejectValueIsIncorrect, SessionRejectOther,
		SessionRejectSystemIsUnavailable, SessionRejectClOrdIDIsNotUnique} {
		m.sessionRej[r] = f.Counter("moex_twime_session_rejects_total", "reason", r.String())
	}
	for c := TerminationFinished; c <= TerminationSequenceReset; c++ {
		m.terminates[c] = f.Counter("moex_twime_terminates_total", "code", c.String())
	}
	for c := EstablishRejectUnnegotiated; c <= EstablishRejectTooFastReconnect; c++ {
		m.establishRej[c] = f.Counter("moex_twime_establish_rejects_total", "code", c.String())
	}
	return m
}

func (m *metrics) onSent(id uint16) {
	if c, ok := m.sent[id]; ok {
		c.Inc()
		return
	}
	m.sentUnknown.Inc()
}

func (m *metrics) onReceived(id uint16) {
	if c, ok := m.received[id]; ok {
		c.Inc()
		return
	}
	m.recvUnknown.Inc()
}

func (m *metrics) onSessionReject(r SessionRejectReason) {
	if c, ok := m.sessionRej[r]; ok {
		c.Inc()
		return
	}
	m.sessionRejX.Inc()
}

func (m *metrics) onTerminate(c TerminationCode) {
	if k, ok := m.terminates[c]; ok {
		k.Inc()
		return
	}
	m.terminatesX.Inc()
}

func (m *metrics) onEstablishReject(c EstablishmentRejectCode) {
	if k, ok := m.establishRej[c]; ok {
		k.Inc()
		return
	}
	m.establishRejX.Inc()
}

// codeLabel — decimal rendering for log fields.
func codeLabel(v uint8) string { return strconv.Itoa(int(v)) }
