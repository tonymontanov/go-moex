package pcap

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"
)

func TestWriterReaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	var w *Writer
	var err error
	w, err = NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var src [4]byte = [4]byte{91, 203, 253, 244}
	var dst [4]byte = [4]byte{239, 195, 20, 81}
	var ts time.Time = time.Date(2026, 5, 15, 6, 59, 0, 123456789, time.UTC)
	if err = w.WriteUDP(ts, src, dst, 20081, 20081, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err = w.WriteUDP(ts.Add(time.Second), src, dst, 1, 2, []byte{}); err != nil {
		t.Fatal(err)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}

	var rd *Reader
	rd, err = NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var pkt Packet
	pkt, err = rd.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(pkt.Payload) != "hello" || pkt.DstPort != 20081 || pkt.DstIP != dst || pkt.SrcIP != src {
		t.Fatalf("unexpected packet: %+v", pkt)
	}
	if !pkt.TS.Equal(ts) {
		t.Fatalf("timestamp %s != %s", pkt.TS, ts)
	}
	pkt, err = rd.Next()
	if err != nil || len(pkt.Payload) != 0 || pkt.DstPort != 2 {
		t.Fatalf("second packet: %+v err=%v", pkt, err)
	}
	if _, err = rd.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if st := rd.Stats(); st.Records != 2 || st.UDP != 2 {
		t.Fatalf("stats %+v", st)
	}
}

func TestReaderSkipsNonUDPAndFragments(t *testing.T) {
	var buf bytes.Buffer
	var w *Writer
	var err error
	w, err = NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var src, dst [4]byte = [4]byte{1, 1, 1, 1}, [4]byte{2, 2, 2, 2}
	if err = w.WriteUDP(time.Unix(1, 0), src, dst, 5, 6, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	// Corrupt the record: make it TCP, then append a fragment copy.
	var raw []byte = buf.Bytes()
	var ipOff int = globalHdrSize + recordHdrSize + 14
	var rec []byte = append([]byte{}, raw[globalHdrSize:]...)
	raw[ipOff+9] = 6 // TCP
	var frag []byte = append([]byte{}, rec...)
	binary.BigEndian.PutUint16(frag[recordHdrSize+14+6:recordHdrSize+14+8], 0x2000) // MF
	raw = append(raw, frag...)
	raw = append(raw, rec...) // one good record

	var rd *Reader
	rd, err = NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	var pkt Packet
	pkt, err = rd.Next()
	if err != nil || string(pkt.Payload) != "a" || pkt.Index != 2 {
		t.Fatalf("expected the third record, got %+v err=%v", pkt, err)
	}
	if _, err = rd.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	var st Stats = rd.Stats()
	if st.SkippedProto != 1 || st.Fragments != 1 || st.UDP != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestReaderRejectsPcapNG(t *testing.T) {
	var hdr [globalHdrSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], magicPcapNG)
	if _, err := NewReader(bytes.NewReader(hdr[:])); err != ErrPcapNG {
		t.Fatalf("expected ErrPcapNG, got %v", err)
	}
}
