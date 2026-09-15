package twime_test

import (
	"context"
	"testing"
	"time"

	"github.com/tonymontanov/go-moex/internal/twime"
	"github.com/tonymontanov/go-moex/internal/twime/twimetest"
)

// Certification steps of the exchange's sample scripts
// (TWIME/Spectra/prod/samples/twime_certification.zip, cert_*.py): the
// same messages with the same field values, replayed through a twime.Session
// against the fake gateway. Steps 1.1 (heartbeat timeout) and 1.2
// (reconnect after twime.Terminate) are exercised by TestSessionHeartbeat /
// TestSessionTerminateHandshake; the table below covers the order-entry
// steps so that the live certification run (after credentials arrive)
// can reuse it verbatim against the real gateway.
//
// The sample scripts never assert on responses (they only print), so the
// only checks here are that every step is encoded, admitted by the pacer
// and framed correctly on the wire.
var certScenarios = []struct {
	name  string
	steps []twime.Marshaler
}{
	{"cert_2_1 limit order", []twime.Marshaler{twime.NewOrderSingle{ClOrdID: 100, Account: "5000061", Price: 3319100000,
		OrderQty: 5, Side: twime.SideSell, TimeInForce: twime.TimeInForceDay, SecurityID: 305203, ExpireDate: twime.NullTimestamp,
		ClOrdLinkID: 7895424, ComplianceID: twime.ComplianceAutofollow}}},
	{"cert_2_2 GTD order", []twime.Marshaler{twime.NewOrderSingle{ClOrdID: 111, Account: "5000061", Price: 3319100000,
		OrderQty: 5, Side: twime.SideBuy, TimeInForce: twime.TimeInForceGTD, SecurityID: 305203, ExpireDate: 1701392400000000000,
		ClOrdLinkID: 7895424, ComplianceID: twime.ComplianceNotAvailable}}},
	{"cert_2_3 cancel", []twime.Marshaler{twime.OrderCancelRequest{ClOrdID: 101, OrderID: 1234567890123456789,
		SecurityID: 305203, Account: "5000061"}}},
	{"cert_2_4 order with client flags", []twime.Marshaler{twime.NewOrderSingle{ClOrdID: 1454, Account: "5000061",
		Price: 700000, OrderQty: 5, Side: twime.SideBuy, TimeInForce: twime.TimeInForceDay, SecurityID: 227271,
		ExpireDate: twime.NullTimestamp, ClientFlags: 0, ComplianceID: twime.ComplianceNotAvailable}}},
	{"cert_2_5 replace", []twime.Marshaler{
		twime.NewOrderSingle{ClOrdID: 133, Account: "5000061", Price: 3319100000, OrderQty: 5, Side: twime.SideSell,
			TimeInForce: twime.TimeInForceDay, SecurityID: 305203, ExpireDate: twime.NullTimestamp,
			ClientFlags: twime.ClientFlagDontCheckLimits, ComplianceID: twime.ComplianceNotAvailable},
		twime.OrderReplaceRequest{ClOrdID: 134, OrderID: 1234567890123456789, Price: 95, OrderQty: 5,
			SecurityID: 305203, Mode: twime.ReplaceModeDontChangeOrderQty, Account: "5000061",
			ComplianceID: twime.ComplianceNotAvailable}}},
	{"cert_2_6 another instrument", []twime.Marshaler{twime.NewOrderSingle{ClOrdID: 104, Account: "5000061", Price: 500000,
		OrderQty: 5, Side: twime.SideBuy, TimeInForce: twime.TimeInForceDay, SecurityID: 240193, ExpireDate: twime.NullTimestamp,
		ComplianceID: twime.ComplianceNotAvailable}}},
	{"cert_2_7 mass cancel by security type", []twime.Marshaler{
		twime.OrderMassCancelRequest{ClOrdID: 105, SecurityID: twime.NullInt32, SecurityType: twime.SecurityTypeFuture,
			Side: twime.SideAllOrders, Account: "5000061"},
		twime.OrderMassCancelRequest{ClOrdID: 105, SecurityID: twime.NullInt32, SecurityType: twime.SecurityTypeOption,
			Side: twime.SideAllOrders, Account: "5000061"},
		twime.OrderMassCancelRequest{ClOrdID: 105, SecurityID: twime.NullInt32, SecurityType: twime.SecurityTypeMultileg,
			Side: twime.SideAllOrders, Account: "5000061"},
		twime.OrderMassCancelRequest{ClOrdID: 105, SecurityID: twime.NullInt32,
			SecurityType: twime.SecurityTypeFuture | twime.SecurityTypeOption | twime.SecurityTypeMultileg,
			Side:         twime.SideAllOrders, Account: "5000061"}}},
	{"cert_2_8 retransmit request", []twime.Marshaler{twime.RetransmitRequest{FromSeqNo: 1, Count: 5}}},
	{"cert_4_1 mass cancel by BF limit", []twime.Marshaler{twime.OrderMassCancelByBFLimitRequest{ClOrdID: 106, Account: "5000061"}}},
	{"cert_5_1 NCC order", []twime.Marshaler{twime.NewOrderSingle{ClOrdID: 100, Account: "5000061", Price: 3319100000,
		OrderQty: 5, Side: twime.SideSell, TimeInForce: twime.TimeInForceDay, SecurityID: 305203, ExpireDate: twime.NullTimestamp,
		ClientFlags: twime.ClientFlagNccRequest, ComplianceID: twime.ComplianceNotAvailable}}},
	{"cert_5_3 NCC cancel", []twime.Marshaler{twime.OrderCancelRequest{ClOrdID: 11, OrderID: 12938, SecurityID: 305203,
		ClientFlags: twime.ClientFlagNccRequest, Account: "5000061"}}},
	{"cert_5_4 NCC replace", []twime.Marshaler{
		twime.NewOrderSingle{ClOrdID: 133, Account: "5000061", Price: 8492400000, OrderQty: 5, Side: twime.SideSell,
			TimeInForce: twime.TimeInForceDay, SecurityID: 305203, ExpireDate: twime.NullTimestamp,
			ClientFlags: twime.ClientFlagNccRequest, ComplianceID: twime.ComplianceNotAvailable},
		twime.OrderReplaceRequest{ClOrdID: 134, OrderID: 1234567890123456789, Price: 8492400000, OrderQty: 5,
			SecurityID: 305203, Mode: twime.ReplaceModeDontChangeOrderQty, ClientFlags: twime.ClientFlagNccRequest,
			Account: "5000061", ComplianceID: twime.ComplianceNotAvailable}}},
	{"cert_5_5 iceberg", []twime.Marshaler{twime.NewOrderIceberg{ClOrdID: 1115, Account: "5000061", Price: 3319100000,
		OrderQty: 1000, DisplayQty: 100, DisplayVarianceQty: 2, Side: twime.SideSell, SecurityID: 305203,
		ExpireDate: twime.NullTimestamp, ComplianceID: twime.ComplianceNotAvailable}}},
	{"cert_5_6 iceberg GTD", []twime.Marshaler{twime.NewOrderIceberg{ClOrdID: 1115, Account: "5000061", Price: 3319100000,
		OrderQty: 1000, DisplayQty: 100, DisplayVarianceQty: 2, Side: twime.SideSell, SecurityID: 305203,
		ExpireDate: 1701392400000000000, ComplianceID: twime.ComplianceNotAvailable}}},
	{"cert_5_7 iceberg cancel", []twime.Marshaler{twime.OrderIcebergCancelRequest{ClOrdID: 101, OrderID: 1234567890123456789,
		SecurityID: 305203, Account: "5000061"}}},
	{"cert_5_8 iceberg replace", []twime.Marshaler{
		twime.NewOrderIceberg{ClOrdID: 1115, Account: "5000061", Price: 3319100000, OrderQty: 1000, DisplayQty: 100,
			DisplayVarianceQty: 2, Side: twime.SideSell, SecurityID: 305203, ExpireDate: twime.NullTimestamp,
			ComplianceID: twime.ComplianceNotAvailable},
		twime.OrderIcebergReplaceRequest{ClOrdID: 1116, OrderID: 1234567890123456789, Price: 95, SecurityID: 305203,
			Account: "5000061", ComplianceID: twime.ComplianceNotAvailable}}},
	{"cert_5_9 iceberg BOC", []twime.Marshaler{twime.NewOrderIcebergX{ClOrdID: 1115, Account: "5000061", Price: 3319100000,
		OrderQty: 1000, DisplayQty: 100, DisplayVarianceQty: 2, Side: twime.SideSell, TimeInForce: twime.TimeInForceBOC,
		SecurityID: 305203, ExpireDate: twime.NullTimestamp, ComplianceID: twime.ComplianceNotAvailable}}},
}

func TestCertificationSteps(t *testing.T) {
	g := twimetest.New(t)
	s, _ := dialTest(t, g, twime.Config{KeepaliveInterval: 60 * time.Second, TradingRate: 3000})
	g.Expect(twime.TemplateEstablish, time.Second)
	ctx := context.Background()
	for _, sc := range certScenarios {
		for i, step := range sc.steps {
			if err := s.Send(ctx, step); err != nil {
				t.Fatalf("%s step %d: %v", sc.name, i, err)
			}
			f := g.Expect(step.Template(), time.Second)
			want := step.Append(nil)
			if int(f.Header.BlockLength) != len(want)-8 || len(f.Body) != len(want)-8 {
				t.Fatalf("%s step %d: framed %d body bytes, encoder produced %d", sc.name, i, len(f.Body), len(want)-8)
			}
			if string(f.Body) != string(want[8:]) {
				t.Fatalf("%s step %d: wire body differs from encoder output", sc.name, i)
			}
		}
	}
	tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.Terminate(tctx); err != nil {
		t.Fatalf("terminate: %v", err)
	}
}
