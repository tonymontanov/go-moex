package simba

import "testing"

// BenchmarkParsePacket — decode cost of a production-shaped packet
// (BestPrices + OrderUpdate + 2×OrderExecution) including field access.
func BenchmarkParsePacket(b *testing.B) {
	var bp []byte = []byte{36, 0, 1}
	bp = append(bp, make([]byte, 36)...)
	var packet []byte = incrementalPacket(105806, FlagLastFragment,
		sbeMessage(TemplateBestPrices, 0, bp),
		sbeMessage(TemplateOrderUpdate, orderUpdateSize, orderUpdateBody(1439162, 60142, 1892945606659163299, 77667, 26, MDUpdateActionNew, MDEntryTypeBid)),
		sbeMessage(TemplateOrderExecution, orderExecutionSize, orderExecutionBody(1439162, 60144, 1892945606659163299, 77667, 0, 77664, 26, 5, 0x20000000002, MDUpdateActionDelete, MDEntryTypeBid)),
		sbeMessage(TemplateOrderExecution, orderExecutionSize, orderExecutionBody(1439162, 60145, 1892945606659163300, 77664, 0, 77664, 26, 5, 0x40000000001, MDUpdateActionDelete, MDEntryTypeOffer)),
	)
	b.ReportAllocs()
	b.SetBytes(int64(len(packet)))
	var sink int64
	for i := 0; i < b.N; i++ {
		var p Packet
		var err error
		p, err = ParsePacket(packet, 0)
		if err != nil {
			b.Fatal(err)
		}
		for {
			var m Message
			var ok bool
			m, ok, err = p.Next()
			if err != nil || !ok {
				break
			}
			switch m.Kind {
			case KindOrderUpdate:
				var u OrderUpdate
				u, _ = m.OrderUpdate()
				sink += u.MDEntrySize
			case KindOrderExecution:
				var e OrderExecution
				e, _ = m.OrderExecution()
				sink += e.LastQty
			case KindBestPrices:
				var v BestPricesView
				v, _ = m.BestPrices()
				sink += int64(v.Len())
			}
		}
	}
	if sink == 0 {
		b.Fatal("sink")
	}
}
