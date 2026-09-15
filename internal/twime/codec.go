/*
FILE: internal/twime/codec.go

DESCRIPTION:
TCP framing for TWIME. There is no length prefix (no SOFH): every frame
is an 8-byte SBE messageHeader followed by exactly BlockLength bytes of
root block (spec §3, FAQ §3.2). A frame boundary is therefore known only
after the header is read, so the Reader consumes the stream in two fixed
reads per message, both into a buffer it owns — zero allocations in the
steady state.

Forward compatibility: the schema id must match (anything else is a
different protocol and the session is torn down), but a root block longer
than the codec knows is accepted and the tail ignored, per the SBE rule
that new fields are appended. A root block SHORTER than expected is an
error of the specific decoder (ErrShortBlock), not of the Reader.
*/
package twime

import (
	"bufio"
	"fmt"
	"io"

	"github.com/tonymontanov/go-moex/internal/sbe"
)

// Frame — one message as read off the wire. Body aliases the Reader's
// buffer and is valid only until the next ReadFrame call.
type Frame struct {
	Header sbe.MessageHeader
	Body   []byte
	// SeqNo — server-assigned sequence number of an application-layer
	// message (0 for session-layer frames, which are not numbered).
	SeqNo uint64
	// Retransmitted — the frame arrived inside a Retransmission block, not
	// live.
	Retransmitted bool
}

// Template — shorthand for Header.TemplateID.
func (f Frame) Template() uint16 { return f.Header.TemplateID }

// ErrSchemaMismatch — a frame carried a schemaId other than SchemaID.
type ErrSchemaMismatch struct {
	Got uint16
}

func (e *ErrSchemaMismatch) Error() string {
	return fmt.Sprintf("twime: schema id %d on the wire, codec speaks %d", e.Got, SchemaID)
}

// Reader — frame reader over a TCP stream.
type Reader struct {
	br  *bufio.Reader
	hdr [sbe.HeaderSize]byte
	buf []byte
}

// NewReader wraps r. The body buffer starts at MaxBlockLength and grows
// only if a newer schema ships a larger root block.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 64<<10), buf: make([]byte, MaxBlockLength)}
}

// ReadFrame blocks until one full frame is available. io.EOF is returned
// as-is on a clean boundary; a truncated frame yields
// io.ErrUnexpectedEOF.
func (r *Reader) ReadFrame() (Frame, error) {
	if _, err := io.ReadFull(r.br, r.hdr[:]); err != nil {
		return Frame{}, err
	}
	h, _ := sbe.DecodeHeader(r.hdr[:])
	if h.SchemaID != SchemaID {
		return Frame{}, &ErrSchemaMismatch{Got: h.SchemaID}
	}
	n := int(h.BlockLength)
	if n > len(r.buf) {
		r.buf = make([]byte, n)
	}
	body := r.buf[:n]
	if _, err := io.ReadFull(r.br, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return Frame{}, err
	}
	return Frame{Header: h, Body: body}, nil
}

// Typed accessors — thin wrappers over the Decode* functions so a handler
// can switch on f.Template() and pull the struct without touching Body.

func (f Frame) EstablishmentAck() (EstablishmentAck, error) { return DecodeEstablishmentAck(f.Body) }
func (f Frame) EstablishmentReject() (EstablishmentReject, error) {
	return DecodeEstablishmentReject(f.Body)
}
func (f Frame) Terminate() (Terminate, error)           { return DecodeTerminate(f.Body) }
func (f Frame) Retransmission() (Retransmission, error) { return DecodeRetransmission(f.Body) }
func (f Frame) Sequence() (Sequence, error)             { return DecodeSequence(f.Body) }
func (f Frame) FloodReject() (FloodReject, error)       { return DecodeFloodReject(f.Body) }
func (f Frame) SessionReject() (SessionReject, error)   { return DecodeSessionReject(f.Body) }
func (f Frame) BusinessMessageReject() (BusinessMessageReject, error) {
	return DecodeBusinessMessageReject(f.Body)
}
func (f Frame) NewOrderSingleResponse() (NewOrderSingleResponse, error) {
	return DecodeNewOrderSingleResponse(f.Body)
}
func (f Frame) NewOrderIcebergResponse() (NewOrderIcebergResponse, error) {
	return DecodeNewOrderIcebergResponse(f.Body)
}
func (f Frame) OrderCancelResponse() (OrderCancelResponse, error) {
	return DecodeOrderCancelResponse(f.Body)
}
func (f Frame) OrderReplaceResponse() (OrderReplaceResponse, error) {
	return DecodeOrderReplaceResponse(f.Body)
}
func (f Frame) OrderMassCancelResponse() (OrderMassCancelResponse, error) {
	return DecodeOrderMassCancelResponse(f.Body)
}
func (f Frame) ExecutionSingleReport() (ExecutionSingleReport, error) {
	return DecodeExecutionSingleReport(f.Body)
}
func (f Frame) ExecutionMultilegReport() (ExecutionMultilegReport, error) {
	return DecodeExecutionMultilegReport(f.Body)
}
func (f Frame) EmptyBook() (EmptyBook, error)     { return DecodeEmptyBook(f.Body) }
func (f Frame) SystemEvent() (SystemEvent, error) { return DecodeSystemEvent(f.Body) }
