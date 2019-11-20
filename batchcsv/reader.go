package batchcsv

import (
	"log"
	"os"

	"github.com/smartystreets/scanners/csv"
)

// Reader wraps batch csv reader
type Reader struct {
	f   *os.File
	sc  *csv.Scanner
	cfg BatchConfig
}

// NewReader creates a new batch reader
func NewReader(fileName string, cfg BatchConfig) (*Reader, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		f:   f,
		sc:  csv.NewScanner(f),
		cfg: cfg,
	}
	return r, nil
}

// Read outputs CSV rows to an output channel
func (r *Reader) Read() (out chan Any) {
	out = make(chan Any, r.cfg.OutputBuffer)

	go func(out chan Any) {
		for r.sc.Scan() {
			if err := r.sc.Error(); err != nil {
				log.Panic("error while scanning csv:", err)
			}

			out <- r.sc.Record()
		}
		close(out)
	}(out)

	return out
}

// Close closes the reader
func (r *Reader) Close() error {
	return r.f.Close()
}
