/*
FILE: internal/sbe/sbe.go

DESCRIPTION:
Primitives shared by every MOEX SPECTRA SBE schema (SIMBA market data,
TWIME order entry): the 8-byte messageHeader composite, the Decimal5
composite (int64 mantissa, constant exponent -5) and the little-endian
fixed-width accessors the per-schema codecs are built from.

Both schemas declare byteOrder="littleEndian" and an identical
messageHeader (blockLength/templateId/schemaId/version, uint16 each), so
the framing code can live here once. Null sentinels are deliberately NOT
here: SIMBA and TWIME disagree on them (SIMBA Int64NULL = min int64 per
the SBE default, TWIME Int64 nullValue = max int64), so each schema
package declares its own.

Everything is allocation-free: encoders append into caller-owned
buffers, decoders read fixed offsets off the input slice.
*/
package sbe

import (
	"encoding/binary"
	"errors"
)

// HeaderSize — wire size of the messageHeader composite (4 x uint16).
const HeaderSize = 8

// ErrShort — input shorter than the fixed-size structure being decoded.
var ErrShort = errors.New("sbe: buffer too short")

// MessageHeader — composite messageHeader, identical in SIMBA and TWIME.
type MessageHeader struct {
	BlockLength uint16
	TemplateID  uint16
	SchemaID    uint16
	Version     uint16
}

// DecodeHeader reads the 8-byte messageHeader from the front of b.
func DecodeHeader(b []byte) (MessageHeader, error) {
	if len(b) < HeaderSize {
		return MessageHeader{}, ErrShort
	}
	return MessageHeader{
		BlockLength: binary.LittleEndian.Uint16(b[0:2]),
		TemplateID:  binary.LittleEndian.Uint16(b[2:4]),
		SchemaID:    binary.LittleEndian.Uint16(b[4:6]),
		Version:     binary.LittleEndian.Uint16(b[6:8]),
	}, nil
}

// AppendHeader appends the 8-byte messageHeader to dst.
func AppendHeader(dst []byte, h MessageHeader) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, h.BlockLength)
	dst = binary.LittleEndian.AppendUint16(dst, h.TemplateID)
	dst = binary.LittleEndian.AppendUint16(dst, h.SchemaID)
	dst = binary.LittleEndian.AppendUint16(dst, h.Version)
	return dst
}

// Decimal5Exponent — constant exponent of the Decimal5 composite. The
// wire carries only the int64 mantissa; value = mantissa * 10^-5.
const Decimal5Exponent = -5

// Decimal5Scale — 10^5, the divisor to turn a Decimal5 mantissa into a
// real number (and the multiplier for the reverse conversion).
const Decimal5Scale int64 = 100_000

// Little-endian fixed-width readers. Callers guarantee bounds (they
// decode a fixed root block whose length was checked once up front).

func U8(b []byte, off int) uint8   { return b[off] }
func U16(b []byte, off int) uint16 { return binary.LittleEndian.Uint16(b[off:]) }
func U32(b []byte, off int) uint32 { return binary.LittleEndian.Uint32(b[off:]) }
func U64(b []byte, off int) uint64 { return binary.LittleEndian.Uint64(b[off:]) }
func I8(b []byte, off int) int8    { return int8(b[off]) }
func I16(b []byte, off int) int16  { return int16(binary.LittleEndian.Uint16(b[off:])) }
func I32(b []byte, off int) int32  { return int32(binary.LittleEndian.Uint32(b[off:])) }
func I64(b []byte, off int) int64  { return int64(binary.LittleEndian.Uint64(b[off:])) }

// Little-endian fixed-width writers at an absolute offset. Callers
// guarantee len(b) >= off + width.

func PutU8(b []byte, off int, v uint8)   { b[off] = v }
func PutU16(b []byte, off int, v uint16) { binary.LittleEndian.PutUint16(b[off:], v) }
func PutU32(b []byte, off int, v uint32) { binary.LittleEndian.PutUint32(b[off:], v) }
func PutU64(b []byte, off int, v uint64) { binary.LittleEndian.PutUint64(b[off:], v) }
func PutI8(b []byte, off int, v int8)    { b[off] = uint8(v) }
func PutI16(b []byte, off int, v int16)  { binary.LittleEndian.PutUint16(b[off:], uint16(v)) }
func PutI32(b []byte, off int, v int32)  { binary.LittleEndian.PutUint32(b[off:], uint32(v)) }
func PutI64(b []byte, off int, v int64)  { binary.LittleEndian.PutUint64(b[off:], uint64(v)) }

// PutChars writes a fixed-length char array: s truncated to n bytes and
// padded with NUL (the SBE convention for char arrays; MOEX's own Python
// serializer uses struct "Ns" which pads with NUL as well).
func PutChars(b []byte, off, n int, s string) {
	i := copy(b[off:off+n], s)
	for ; i < n; i++ {
		b[off+i] = 0
	}
}

// Chars reads a fixed-length char array, trimming trailing NUL and space
// padding. Allocates (string conversion); use CharsBytes on hot paths.
func Chars(b []byte, off, n int) string {
	return string(CharsBytes(b, off, n))
}

// CharsBytes returns the sub-slice of a fixed-length char array without
// trailing NUL/space padding. Aliases b — valid only as long as b is.
func CharsBytes(b []byte, off, n int) []byte {
	s := b[off : off+n]
	for len(s) > 0 && (s[len(s)-1] == 0 || s[len(s)-1] == ' ') {
		s = s[:len(s)-1]
	}
	return s
}
