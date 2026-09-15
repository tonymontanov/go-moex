/*
FILE: internal/twime/encode_server.go

DESCRIPTION:
Append encoders for server-to-client messages. A client never sends
these; they exist so that the in-package fake gateway (session_test.go),
the golden codec test and any future gateway simulator can produce
byte-exact frames from the same offset tables the decoders read. Keeping
them next to the decoders means a layout change is made in one place and
caught by the round-trip test.
*/
package twime

import "github.com/tonymontanov/go-moex/internal/sbe"

func (EstablishmentAck) Template() uint16 { return TemplateEstablishmentAck }

func (m EstablishmentAck) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateEstablishmentAck, BlockLengthEstablishmentAck)
	sbe.PutU64(b, 0, m.RequestTimestamp)
	sbe.PutU32(b, 8, m.KeepaliveInterval)
	sbe.PutU64(b, 12, m.NextSeqNo)
	return dst
}

func (EstablishmentReject) Template() uint16 { return TemplateEstablishmentReject }

func (m EstablishmentReject) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateEstablishmentReject, BlockLengthEstablishmentReject)
	sbe.PutU64(b, 0, m.RequestTimestamp)
	sbe.PutU8(b, 8, uint8(m.Code))
	return dst
}

func (Retransmission) Template() uint16 { return TemplateRetransmission }

func (m Retransmission) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateRetransmission, BlockLengthRetransmission)
	sbe.PutU64(b, 0, m.NextSeqNo)
	sbe.PutU64(b, 8, m.RequestTimestamp)
	sbe.PutU32(b, 16, m.Count)
	return dst
}

func (FloodReject) Template() uint16 { return TemplateFloodReject }

func (m FloodReject) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateFloodReject, BlockLengthFloodReject)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU32(b, 8, m.QueueSize)
	sbe.PutU32(b, 12, m.PenaltyRemain)
	return dst
}

func (SessionReject) Template() uint16 { return TemplateSessionReject }

func (m SessionReject) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateSessionReject, BlockLengthSessionReject)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU32(b, 8, m.RefTagID)
	sbe.PutU8(b, 12, uint8(m.Reason))
	return dst
}

func (BusinessMessageReject) Template() uint16 { return TemplateBusinessMessageReject }

func (m BusinessMessageReject) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateBusinessMessageReject, BlockLengthBusinessMessageReject)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.Timestamp)
	sbe.PutI32(b, 16, m.OrdRejReason)
	return dst
}

func (NewOrderSingleResponse) Template() uint16 { return TemplateNewOrderSingleResponse }

func (m NewOrderSingleResponse) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateNewOrderSingleResponse, BlockLengthNewOrderSingleResponse)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.Timestamp)
	sbe.PutU64(b, 16, m.ExpireDate)
	sbe.PutI64(b, 24, m.OrderID)
	sbe.PutU64(b, 32, uint64(m.Flags))
	sbe.PutU64(b, 40, m.Flags2)
	sbe.PutI64(b, 48, m.Price)
	sbe.PutI32(b, 56, m.SecurityID)
	sbe.PutU32(b, 60, m.OrderQty)
	sbe.PutI32(b, 64, m.TradingSessionID)
	sbe.PutI32(b, 68, m.ClOrdLinkID)
	sbe.PutU8(b, 72, uint8(m.Side))
	sbe.PutU8(b, 73, uint8(m.ComplianceID))
	return dst
}

func (NewOrderIcebergResponse) Template() uint16 { return TemplateNewOrderIcebergResponse }

func (m NewOrderIcebergResponse) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateNewOrderIcebergResponse, BlockLengthNewOrderIcebergResponse)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.Timestamp)
	sbe.PutU64(b, 16, m.ExpireDate)
	sbe.PutI64(b, 24, m.OrderID)
	sbe.PutI64(b, 32, m.DisplayOrderID)
	sbe.PutU64(b, 40, uint64(m.Flags))
	sbe.PutU64(b, 48, m.Flags2)
	sbe.PutI64(b, 56, m.Price)
	sbe.PutI32(b, 64, m.SecurityID)
	sbe.PutU32(b, 68, m.OrderQty)
	sbe.PutU32(b, 72, m.DisplayQty)
	sbe.PutU32(b, 76, m.DisplayVarianceQty)
	sbe.PutI32(b, 80, m.TradingSessionID)
	sbe.PutI32(b, 84, m.ClOrdLinkID)
	sbe.PutU8(b, 88, uint8(m.Side))
	sbe.PutU8(b, 89, uint8(m.ComplianceID))
	return dst
}

func (OrderCancelResponse) Template() uint16 { return TemplateOrderCancelResponse }

func (m OrderCancelResponse) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateOrderCancelResponse, BlockLengthOrderCancelResponse)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.Timestamp)
	sbe.PutI64(b, 16, m.OrderID)
	sbe.PutU64(b, 24, uint64(m.Flags))
	sbe.PutU64(b, 32, m.Flags2)
	sbe.PutU32(b, 40, m.OrderQty)
	sbe.PutI32(b, 44, m.TradingSessionID)
	sbe.PutI32(b, 48, m.ClOrdLinkID)
	return dst
}

func (OrderReplaceResponse) Template() uint16 { return TemplateOrderReplaceResponse }

func (m OrderReplaceResponse) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateOrderReplaceResponse, BlockLengthOrderReplaceResponse)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.Timestamp)
	sbe.PutI64(b, 16, m.OrderID)
	sbe.PutI64(b, 24, m.PrevOrderID)
	sbe.PutU64(b, 32, uint64(m.Flags))
	sbe.PutU64(b, 40, m.Flags2)
	sbe.PutI64(b, 48, m.Price)
	sbe.PutU32(b, 56, m.OrderQty)
	sbe.PutI32(b, 60, m.TradingSessionID)
	sbe.PutI32(b, 64, m.ClOrdLinkID)
	sbe.PutU8(b, 68, uint8(m.ComplianceID))
	return dst
}

func (OrderMassCancelResponse) Template() uint16 { return TemplateOrderMassCancelResponse }

func (m OrderMassCancelResponse) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateOrderMassCancelResponse, BlockLengthOrderMassCancelResponse)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.Timestamp)
	sbe.PutI32(b, 16, m.TotalAffectedOrders)
	return dst
}

func (ExecutionSingleReport) Template() uint16 { return TemplateExecutionSingleReport }

func (m ExecutionSingleReport) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateExecutionSingleReport, BlockLengthExecutionSingleReport)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.Timestamp)
	sbe.PutI64(b, 16, m.OrderID)
	sbe.PutI64(b, 24, m.TrdMatchID)
	sbe.PutU64(b, 32, uint64(m.Flags))
	sbe.PutU64(b, 40, m.Flags2)
	sbe.PutI64(b, 48, m.LastPx)
	sbe.PutU32(b, 56, m.LastQty)
	sbe.PutU32(b, 60, m.OrderQty)
	sbe.PutI32(b, 64, m.TradingSessionID)
	sbe.PutI32(b, 68, m.ClOrdLinkID)
	sbe.PutI32(b, 72, m.SecurityID)
	sbe.PutU8(b, 76, uint8(m.Side))
	return dst
}

func (ExecutionMultilegReport) Template() uint16 { return TemplateExecutionMultilegReport }

func (m ExecutionMultilegReport) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateExecutionMultilegReport, BlockLengthExecutionMultilegReport)
	sbe.PutU64(b, 0, m.ClOrdID)
	sbe.PutU64(b, 8, m.Timestamp)
	sbe.PutI64(b, 16, m.OrderID)
	sbe.PutI64(b, 24, m.TrdMatchID)
	sbe.PutU64(b, 32, uint64(m.Flags))
	sbe.PutU64(b, 40, m.Flags2)
	sbe.PutI64(b, 48, m.LastPx)
	sbe.PutI64(b, 56, m.LegPrice)
	sbe.PutU32(b, 64, m.LastQty)
	sbe.PutU32(b, 68, m.OrderQty)
	sbe.PutI32(b, 72, m.TradingSessionID)
	sbe.PutI32(b, 76, m.ClOrdLinkID)
	sbe.PutI32(b, 80, m.SecurityID)
	sbe.PutU8(b, 84, uint8(m.Side))
	return dst
}

func (EmptyBook) Template() uint16 { return TemplateEmptyBook }

func (m EmptyBook) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateEmptyBook, BlockLengthEmptyBook)
	sbe.PutU64(b, 0, m.Timestamp)
	sbe.PutI32(b, 8, m.TradingSessionID)
	return dst
}

func (SystemEvent) Template() uint16 { return TemplateSystemEvent }

func (m SystemEvent) Append(dst []byte) []byte {
	dst, b := appendFrame(dst, TemplateSystemEvent, BlockLengthSystemEvent)
	sbe.PutU64(b, 0, m.Timestamp)
	sbe.PutI64(b, 8, m.EventID)
	sbe.PutI32(b, 16, m.TradingSessionID)
	sbe.PutU8(b, 20, uint8(m.TradSesEvent))
	return dst
}
