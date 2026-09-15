package twime

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"
)

// Golden frames produced by the exchange's own Python serializer
// (TWIME/Spectra/prod/samples/twime_certification.zip: schema.py +
// serializer.py, struct format '<' = little-endian, no alignment, char
// arrays NUL-padded). Field values are the ones in goldenCases below;
// regenerate with a script that populates the same values and prints
// binascii.hexlify(msg.serialize()).
var goldenHex = map[string]string{
	"Establish":                       "20008813454d070015cd853dfe9c9717102700005445535400000000000000000000000000000000",
	"EstablishmentAck":                "14008913454d070015cd853dfe9c9717881300002a00000000000000",
	"EstablishmentReject":             "09008a13454d070015cd853dfe9c971704",
	"Terminate":                       "01008b13454d070006",
	"RetransmitRequest":               "14008c13454d070015cd853dfe9c97170a0000000000000005000000",
	"Retransmission":                  "14008d13454d07000a0000000000000015cd853dfe9c971705000000",
	"Sequence":                        "08008e13454d0700ffffffffffffffff",
	"FloodReject":                     "10008f13454d070064000000000000001f00000090d00300",
	"SessionReject":                   "0d009013454d070064000000000000002c00000065",
	"BusinessMessageReject":           "14009113454d0700640000000000000015cd853dfe9c9717ffffffff",
	"NewOrderSingle":                  "2f007017454d07006400000000000000ffffffffffffffff6072d5c50000000033a8040080797800050000004100020035303030303631",
	"OrderCancelRequest":              "1c007617454d070065000000000000001581e97df410221133a804000235303030303631",
	"OrderReplaceRequest":             "2e007717454d070066000000000000001581e97df41022115f00000000000000070000008079780033a8040052000135303030303631",
	"OrderMassCancelRequest":          "32007417454d0700690000000000000000000000ffffff7f07593530303030363100000000000000000000000000000000000000000000000000",
	"OrderMassCancelByBFLimitRequest": "0f007517454d07006a0000000000000035303030303631",
	"NewOrderIceberg":                 "36007817454d07006b00000000000000ffffffffffffffff6072d5c50000000033a80400000000006400000002000000e803000020010035303030303631",
	"NewOrderIcebergX":                "37007b17454d07006b00000000000000ffffffffffffffff6072d5c50000000033a80400000000006400000002000000e8030000207a010035303030303631",
	"OrderIcebergCancelRequest":       "1c007917454d07006c000000000000002b0200000000000033a804000035303030303631",
	"OrderIcebergReplaceRequest":      "29007a17454d07006d000000000000002b020000000000005f000000000000000000000033a80400200035303030303631",
	"NewOrderSingleResponse":          "4a00671b454d0700640000000000000015cd853dfe9c9717ffffffffffffffffb168de3a00000000010000000000000000000000000000006072d5c50000000033a80400050000008f190000807978000241",
	"NewOrderIcebergResponse":         "5a00681b454d07006b0000000000000015cd853dfe9c9717ffffffffffffffffb168de3a00000000b268de3a00000000000000000080000000000000000000006072d5c50000000033a80400e803000064000000020000008f190000000000000120",
	"OrderCancelResponse":             "3400691b454d0700650000000000000015cd853dfe9c9717b168de3a0000000000002000000000000000000000000000050000008f19000080797800",
	"OrderReplaceResponse":            "45006a1b454d0700660000000000000015cd853dfe9c9717b368de3a00000000b168de3a00000000000010000000000000000000000000005f00000000000000070000008f1900008079780052",
	"OrderMassCancelResponse":         "14005f1b454d0700690000000000000015cd853dfe9c971703000000",
	"ExecutionSingleReport":           "4d006b1b454d0700640000000000000015cd853dfe9c9717b168de3a000000000903000000000000010000000004000000000000000000006072d5c50000000002000000030000008f1900008079780033a8040002",
	"ExecutionMultilegReport":         "55006c1b454d0700640000000000000015cd853dfe9c9717b168de3a000000000903000000000000010000000004000000000000000000006072d5c500000000a08601000000000002000000030000008f1900008079780033a8040002",
	"EmptyBook":                       "0c00621b454d070015cd853dfe9c97178f190000",
	"SystemEvent":                     "1500661b454d070015cd853dfe9c97170c000000000000008f19000065",
}

const goldenTS uint64 = 1700000000123456789

// goldenCases — every message of the schema with the values the golden
// frames were generated from. decode, when set, must reproduce msg from
// the golden root block.
var goldenCases = []struct {
	name   string
	msg    Marshaler
	decode func([]byte) (any, error)
}{
	{"Establish", Establish{Timestamp: goldenTS, KeepaliveInterval: 10000, Credentials: "TEST"}, nil},
	{"EstablishmentAck", EstablishmentAck{RequestTimestamp: goldenTS, KeepaliveInterval: 5000, NextSeqNo: 42},
		func(b []byte) (any, error) { return DecodeEstablishmentAck(b) }},
	{"EstablishmentReject", EstablishmentReject{RequestTimestamp: goldenTS, Code: EstablishRejectCredentials},
		func(b []byte) (any, error) { return DecodeEstablishmentReject(b) }},
	{"Terminate", Terminate{Code: TerminationMissedHeartbeat},
		func(b []byte) (any, error) { return DecodeTerminate(b) }},
	{"RetransmitRequest", RetransmitRequest{Timestamp: goldenTS, FromSeqNo: 10, Count: 5}, nil},
	{"Retransmission", Retransmission{NextSeqNo: 10, RequestTimestamp: goldenTS, Count: 5},
		func(b []byte) (any, error) { return DecodeRetransmission(b) }},
	{"Sequence", Sequence{NextSeqNo: NullUint64},
		func(b []byte) (any, error) { return DecodeSequence(b) }},
	{"FloodReject", FloodReject{ClOrdID: 100, QueueSize: 31, PenaltyRemain: 250000},
		func(b []byte) (any, error) { return DecodeFloodReject(b) }},
	{"SessionReject", SessionReject{ClOrdID: 100, RefTagID: 44, Reason: SessionRejectClOrdIDIsNotUnique},
		func(b []byte) (any, error) { return DecodeSessionReject(b) }},
	{"BusinessMessageReject", BusinessMessageReject{ClOrdID: 100, Timestamp: goldenTS, OrdRejReason: -1},
		func(b []byte) (any, error) { return DecodeBusinessMessageReject(b) }},
	{"NewOrderSingle", NewOrderSingle{ClOrdID: 100, ExpireDate: NullTimestamp, Price: 3319100000, SecurityID: 305203,
		ClOrdLinkID: 7895424, OrderQty: 5, ComplianceID: ComplianceAutofollow, TimeInForce: TimeInForceDay,
		Side: SideSell, ClientFlags: 0, Account: "5000061"}, nil},
	{"OrderCancelRequest", OrderCancelRequest{ClOrdID: 101, OrderID: 1234567890123456789, SecurityID: 305203,
		ClientFlags: ClientFlagNccRequest, Account: "5000061"}, nil},
	{"OrderReplaceRequest", OrderReplaceRequest{ClOrdID: 102, OrderID: 1234567890123456789, Price: 95, OrderQty: 7,
		ClOrdLinkID: 7895424, SecurityID: 305203, ComplianceID: ComplianceAlgorithm,
		Mode: ReplaceModeDontChangeOrderQty, ClientFlags: ClientFlagDontCheckLimits, Account: "5000061"}, nil},
	{"OrderMassCancelRequest", OrderMassCancelRequest{ClOrdID: 105, ClOrdLinkID: 0, SecurityID: NullInt32,
		SecurityType: SecurityTypeFuture | SecurityTypeOption | SecurityTypeMultileg, Side: SideAllOrders,
		Account: "5000061", SecurityGroup: ""}, nil},
	{"OrderMassCancelByBFLimitRequest", OrderMassCancelByBFLimitRequest{ClOrdID: 106, Account: "5000061"}, nil},
	{"NewOrderIceberg", NewOrderIceberg{ClOrdID: 107, ExpireDate: NullTimestamp, Price: 3319100000, SecurityID: 305203,
		DisplayQty: 100, DisplayVarianceQty: 2, OrderQty: 1000, ComplianceID: ComplianceNotAvailable, Side: SideBuy,
		Account: "5000061"}, nil},
	{"NewOrderIcebergX", NewOrderIcebergX{ClOrdID: 107, ExpireDate: NullTimestamp, Price: 3319100000, SecurityID: 305203,
		DisplayQty: 100, DisplayVarianceQty: 2, OrderQty: 1000, ComplianceID: ComplianceNotAvailable,
		TimeInForce: TimeInForceBOC, Side: SideBuy, Account: "5000061"}, nil},
	{"OrderIcebergCancelRequest", OrderIcebergCancelRequest{ClOrdID: 108, OrderID: 555, SecurityID: 305203,
		Account: "5000061"}, nil},
	{"OrderIcebergReplaceRequest", OrderIcebergReplaceRequest{ClOrdID: 109, OrderID: 555, Price: 95, SecurityID: 305203,
		ComplianceID: ComplianceNotAvailable, Account: "5000061"}, nil},
	{"NewOrderSingleResponse", NewOrderSingleResponse{ClOrdID: 100, Timestamp: goldenTS, ExpireDate: NullTimestamp,
		OrderID: 987654321, Flags: FlagDay, Price: 3319100000, SecurityID: 305203, OrderQty: 5, TradingSessionID: 6543,
		ClOrdLinkID: 7895424, Side: SideSell, ComplianceID: ComplianceAutofollow},
		func(b []byte) (any, error) { return DecodeNewOrderSingleResponse(b) }},
	{"NewOrderIcebergResponse", NewOrderIcebergResponse{ClOrdID: 107, Timestamp: goldenTS, ExpireDate: NullTimestamp,
		OrderID: 987654321, DisplayOrderID: 987654322, Flags: FlagIceberg, Price: 3319100000, SecurityID: 305203,
		OrderQty: 1000, DisplayQty: 100, DisplayVarianceQty: 2, TradingSessionID: 6543, Side: SideBuy,
		ComplianceID: ComplianceNotAvailable},
		func(b []byte) (any, error) { return DecodeNewOrderIcebergResponse(b) }},
	{"OrderCancelResponse", OrderCancelResponse{ClOrdID: 101, Timestamp: goldenTS, OrderID: 987654321, Flags: FlagCancel,
		OrderQty: 5, TradingSessionID: 6543, ClOrdLinkID: 7895424},
		func(b []byte) (any, error) { return DecodeOrderCancelResponse(b) }},
	{"OrderReplaceResponse", OrderReplaceResponse{ClOrdID: 102, Timestamp: goldenTS, OrderID: 987654323,
		PrevOrderID: 987654321, Flags: FlagReplace, Price: 95, OrderQty: 7, TradingSessionID: 6543, ClOrdLinkID: 7895424,
		ComplianceID: ComplianceAlgorithm},
		func(b []byte) (any, error) { return DecodeOrderReplaceResponse(b) }},
	{"OrderMassCancelResponse", OrderMassCancelResponse{ClOrdID: 105, Timestamp: goldenTS, TotalAffectedOrders: 3},
		func(b []byte) (any, error) { return DecodeOrderMassCancelResponse(b) }},
	{"ExecutionSingleReport", ExecutionSingleReport{ClOrdID: 100, Timestamp: goldenTS, OrderID: 987654321, TrdMatchID: 777,
		Flags: FlagDay | FlagPassiveSide, LastPx: 3319100000, LastQty: 2, OrderQty: 3, TradingSessionID: 6543,
		ClOrdLinkID: 7895424, SecurityID: 305203, Side: SideSell},
		func(b []byte) (any, error) { return DecodeExecutionSingleReport(b) }},
	{"ExecutionMultilegReport", ExecutionMultilegReport{ClOrdID: 100, Timestamp: goldenTS, OrderID: 987654321,
		TrdMatchID: 777, Flags: FlagDay | FlagPassiveSide, LastPx: 3319100000, LegPrice: 100000, LastQty: 2, OrderQty: 3,
		TradingSessionID: 6543, ClOrdLinkID: 7895424, SecurityID: 305203, Side: SideSell},
		func(b []byte) (any, error) { return DecodeExecutionMultilegReport(b) }},
	{"EmptyBook", EmptyBook{Timestamp: goldenTS, TradingSessionID: 6543},
		func(b []byte) (any, error) { return DecodeEmptyBook(b) }},
	{"SystemEvent", SystemEvent{Timestamp: goldenTS, EventID: 12, TradingSessionID: 6543,
		TradSesEvent: TradSesEventSessionDataReady},
		func(b []byte) (any, error) { return DecodeSystemEvent(b) }},
}

func TestGoldenCoversSchema(t *testing.T) {
	if len(goldenCases) != len(goldenHex) {
		t.Fatalf("golden cases %d, golden frames %d", len(goldenCases), len(goldenHex))
	}
	for _, c := range goldenCases {
		if _, ok := goldenHex[c.name]; !ok {
			t.Errorf("no golden frame for %s", c.name)
		}
		if TemplateName(c.msg.Template()) != c.name {
			t.Errorf("%s: Template() = %d (%s)", c.name, c.msg.Template(), TemplateName(c.msg.Template()))
		}
	}
}

func TestGoldenEncode(t *testing.T) {
	for _, c := range goldenCases {
		want, err := hex.DecodeString(goldenHex[c.name])
		if err != nil {
			t.Fatal(err)
		}
		got := c.msg.Append(nil)
		if !bytes.Equal(got, want) {
			t.Errorf("%s encode mismatch\n got %x\nwant %x", c.name, got, want)
		}
	}
}

func TestGoldenDecode(t *testing.T) {
	for _, c := range goldenCases {
		if c.decode == nil {
			continue
		}
		raw, _ := hex.DecodeString(goldenHex[c.name])
		r := NewReader(bytes.NewReader(raw))
		f, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("%s: ReadFrame: %v", c.name, err)
		}
		if f.Template() != c.msg.Template() {
			t.Errorf("%s: template %d", c.name, f.Template())
		}
		if int(f.Header.BlockLength) != len(f.Body) {
			t.Errorf("%s: body %d != blockLength %d", c.name, len(f.Body), f.Header.BlockLength)
		}
		got, err := c.decode(f.Body)
		if err != nil {
			t.Fatalf("%s: decode: %v", c.name, err)
		}
		if !reflect.DeepEqual(got, c.msg) {
			t.Errorf("%s decode mismatch\n got %+v\nwant %+v", c.name, got, c.msg)
		}
	}
}

func TestDecodeShortBlock(t *testing.T) {
	for _, c := range goldenCases {
		if c.decode == nil {
			continue
		}
		raw, _ := hex.DecodeString(goldenHex[c.name])
		body := raw[8 : len(raw)-1]
		if _, err := c.decode(body); err == nil {
			t.Errorf("%s: short block accepted", c.name)
		}
	}
}

func TestReaderStreamAndForwardCompat(t *testing.T) {
	var stream []byte
	for _, c := range goldenCases {
		raw, _ := hex.DecodeString(goldenHex[c.name])
		stream = append(stream, raw...)
	}
	// A frame of a future schema version with a longer root block must be
	// framed correctly and its known prefix decoded.
	future := Sequence{NextSeqNo: 7}.Append(nil)
	future[0] = BlockLengthSequence + 4
	future[6] = 8 // version
	future = append(future, 0xAA, 0xBB, 0xCC, 0xDD)
	stream = append(stream, future...)

	r := NewReader(bytes.NewReader(stream))
	for _, c := range goldenCases {
		f, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if f.Template() != c.msg.Template() {
			t.Fatalf("%s: got template %d", c.name, f.Template())
		}
	}
	f, err := r.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if f.Header.Version != 8 || len(f.Body) != BlockLengthSequence+4 {
		t.Fatalf("future frame: %+v", f.Header)
	}
	seq, err := f.Sequence()
	if err != nil || seq.NextSeqNo != 7 {
		t.Fatalf("future Sequence: %+v %v", seq, err)
	}
	if _, err := r.ReadFrame(); err == nil {
		t.Fatal("expected EOF")
	}
}

func TestReaderSchemaMismatch(t *testing.T) {
	raw := Sequence{NextSeqNo: 1}.Append(nil)
	raw[4] = 0x84 // schemaId 19780 = SIMBA
	raw[5] = 0x4d
	r := NewReader(bytes.NewReader(raw))
	_, err := r.ReadFrame()
	if _, ok := err.(*ErrSchemaMismatch); !ok {
		t.Fatalf("got %v", err)
	}
}

func TestReaderTruncated(t *testing.T) {
	raw := NewOrderSingleResponse{ClOrdID: 1}.Append(nil)
	r := NewReader(bytes.NewReader(raw[:len(raw)-10]))
	if _, err := r.ReadFrame(); err == nil {
		t.Fatal("expected error on truncated body")
	}
}

func TestCodecZeroAlloc(t *testing.T) {
	buf := make([]byte, 0, 256)
	nos := NewOrderSingle{ClOrdID: 1, Price: 100000, SecurityID: 1, OrderQty: 1, Account: "5000061"}
	allocs := testing.AllocsPerRun(1000, func() {
		buf = nos.Append(buf[:0])
	})
	if allocs != 0 {
		t.Errorf("NewOrderSingle.Append allocs = %v", allocs)
	}
	resp := NewOrderSingleResponse{ClOrdID: 1, OrderID: 2}.Append(nil)
	allocs = testing.AllocsPerRun(1000, func() {
		if _, err := DecodeNewOrderSingleResponse(resp[8:]); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("DecodeNewOrderSingleResponse allocs = %v", allocs)
	}
	stream := bytes.Repeat(resp, 1000)
	r := NewReader(bytes.NewReader(stream))
	allocs = testing.AllocsPerRun(999, func() {
		if _, err := r.ReadFrame(); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("Reader.ReadFrame allocs = %v", allocs)
	}
}
