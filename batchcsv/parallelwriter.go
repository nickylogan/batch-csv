package batchcsv

import (
	"sync"

	"github.com/smartystreets/scanners/csv"
)

type parallelWriter struct {
	mux sync.Mutex
	w   *csv.Writer
}

func newParallelWriter(w *csv.Writer) *parallelWriter {
	return &parallelWriter{
		w: w,
	}
}

func (pw *parallelWriter) write(r Record) error {
	defer pw.mux.Unlock()
	pw.mux.Lock()
	err := pw.w.Write(r)
	if err != nil {
		return err
	}

	pw.w.Flush()
	return nil
}
