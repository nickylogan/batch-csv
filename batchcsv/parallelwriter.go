package batchcsv

import (
	"sync"

	"github.com/smartystreets/scanners/csv"
)

// ParallelWriter is a wrapper for a CSV writer with mutex
type ParallelWriter struct {
	mux sync.Mutex
	w   *csv.Writer
}

// NewParallelWriter creates a ParallelWriter instance. It takes a
// csv writer to be wrapped upon
func NewParallelWriter(w *csv.Writer) *ParallelWriter {
	return &ParallelWriter{
		w: w,
	}
}

// Write is a thread-safe csv write. It automatically flushes the
// written values to the file
func (pw *ParallelWriter) Write(r Record) error {
	defer pw.mux.Unlock()
	pw.mux.Lock()
	err := pw.w.Write(r)
	if err != nil {
		return err
	}

	pw.w.Flush()
	return nil
}
