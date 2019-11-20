package batchcsv

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/smartystreets/scanners/csv"
)

// Writer wraps a batch csv writer
type Writer struct {
	f     *os.File
	w     *csv.Writer
	cfg   BatchConfig
	parse Parser
}

type Parser func(a Any) (rec Record, err error)

// NewWriter creates a new batch writer
func NewWriter(fileName string, cfg BatchConfig, p Parser) (*Writer, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		f:     f,
		w:     csv.NewWriter(f),
		cfg:   cfg,
		parse: p,
	}
	return w, nil
}

// Write listens an input channel. It then parses the payload using
// the given parser and writes it to a file
func (w *Writer) Write(in chan Any) (done chan bool) {
	var wg sync.WaitGroup

	jobs := make(chan Job, w.cfg.MaxJobs)
	done = make(chan bool)

	pw := NewParallelWriter(w.w)

	go func(jobs chan Job, in chan Any, pw *ParallelWriter, done chan bool) {
		// spawn a number of workers
		for i := 1; i <= w.cfg.MaxWorkers; i++ {
			go w.work(jobs, &wg, pw)
		}

		// queue jobs
		w.delegateJobs(jobs, in)
		wg.Wait()

		close(jobs)
		done <- true
	}(jobs, in, pw, done)

	return done
}

// Close closes the writer
func (w *Writer) Close() error {
	return w.f.Close()
}

func (w *Writer) work(jobs <-chan Job, wg *sync.WaitGroup, pw *ParallelWriter) {
	for j := range jobs {
		func(j Job) {
			defer wg.Done()
			wg.Add(1)

			rec, err := w.parse(j.Payload)
			if err != nil {
				log.Println("failed to parse:", err)
				return
			}

			err = pw.Write(rec)
			if err != nil {
				log.Println("failed to write:", err)
				return
			}

			fmt.Println("written:", j.Payload)
		}(j)
	}
}

func (w *Writer) delegateJobs(jobs chan<- Job, in <-chan Any) {
	jID := 1
	for d := range in {
		j := Job{
			ID:      jID,
			Payload: d,
		}
		jobs <- j

		jID++
	}
}
