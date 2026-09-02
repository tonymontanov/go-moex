/*
FILE: internal/pcap/open.go

DESCRIPTION:
Open a capture by path: a plain .pcap file, or a .zip archive containing one
(MOEX publishes dumps zipped; the inner pcap is streamed through the
deflate reader without extracting to disk — a 200 MB zip is ~1.5 GB
unpacked and the harness should not need that much scratch space).
*/
package pcap

import (
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
)

// Source — an opened capture; Close releases the underlying file(s).
type Source struct {
	Name   string // file name of the pcap (inner entry name for zips).
	Reader *Reader
	closer []io.Closer
}

// Close closes everything Open opened.
func (s *Source) Close() error {
	var first error
	for i := len(s.closer) - 1; i >= 0; i-- {
		if err := s.closer[i].Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Open opens path: a .pcap, a .pcap.gz, or a .zip with a .pcap/.cap entry
// inside. For zips with several pcap entries, entry selects one by name
// substring; empty picks the first.
func Open(path string, entry string) (*Source, error) {
	var lower string = strings.ToLower(path)
	if !strings.HasSuffix(lower, ".zip") {
		var f *os.File
		var err error
		f, err = os.Open(path)
		if err != nil {
			return nil, err
		}
		var closers []io.Closer = []io.Closer{f}
		var r io.Reader = f
		if strings.HasSuffix(lower, ".gz") {
			var gz *gzip.Reader
			gz, err = gzip.NewReader(f)
			if err != nil {
				_ = f.Close()
				return nil, err
			}
			closers = append(closers, gz)
			r = gz
		}
		var rd *Reader
		rd, err = NewReader(r)
		if err != nil {
			for _, c := range closers {
				_ = c.Close()
			}
			return nil, err
		}
		return &Source{Name: path, Reader: rd, closer: closers}, nil
	}

	var zr *zip.ReadCloser
	var err error
	zr, err = zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	var chosen *zip.File
	for _, zf := range zr.File {
		var lower string = strings.ToLower(zf.Name)
		if zf.FileInfo().IsDir() {
			continue
		}
		if !(strings.HasSuffix(lower, ".pcap") || strings.HasSuffix(lower, ".cap") || strings.HasSuffix(lower, ".pcapng")) {
			continue
		}
		if entry != "" && !strings.Contains(zf.Name, entry) {
			continue
		}
		chosen = zf
		break
	}
	if chosen == nil {
		var names []string
		for _, zf := range zr.File {
			names = append(names, zf.Name)
		}
		_ = zr.Close()
		return nil, fmt.Errorf("pcap: no capture entry in %s (entries: %s)", path, strings.Join(names, ", "))
	}
	var rc io.ReadCloser
	rc, err = chosen.Open()
	if err != nil {
		_ = zr.Close()
		return nil, err
	}
	var rd *Reader
	rd, err = NewReader(rc)
	if err != nil {
		_ = rc.Close()
		_ = zr.Close()
		return nil, fmt.Errorf("%s: %w", chosen.Name, err)
	}
	return &Source{Name: chosen.Name, Reader: rd, closer: []io.Closer{zr, rc}}, nil
}

// ListEntries returns the entry names of a zip (diagnostics).
func ListEntries(path string) ([]string, error) {
	var zr *zip.ReadCloser
	var err error
	zr, err = zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	var out []string
	for _, zf := range zr.File {
		out = append(out, fmt.Sprintf("%s (%d bytes)", zf.Name, zf.UncompressedSize64))
	}
	return out, nil
}
