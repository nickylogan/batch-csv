package batchcsv

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/smartystreets/scanners/csv"
)

type Writer struct {
	File *os.File
	w    *csv.Writer
	cfg  BatchConfig
}

func NewWriter(fileName string, cfg BatchConfig) (*Writer, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		File: f,
		w:    csv.NewWriter(f),
		cfg:  cfg,
	}
	return w, nil
}

func (w *Writer) Write(results chan Record) {
	var wg sync.WaitGroup

	jobs := make(chan Job, w.cfg.MaxJobs)

	pw := newParallelWriter(w.w)

	// spawn a number of workers
	for i := 1; i <= w.cfg.MaxWorkers; i++ {
		go w.work(jobs, &wg, pw)
	}

	// queue jobs
	w.delegateJobs(jobs, results)
	wg.Wait()

	close(jobs)
}

func (w *Writer) work(jobs <-chan Job, wg *sync.WaitGroup, pw *parallelWriter) {
	for j := range jobs {
		func(j Job) {
			defer wg.Done()
			wg.Add(1)

			err := pw.write(j.Record)
			if err != nil {
				log.Println("failed to write:", err)
				return
			}

			fmt.Println("written:", j.ID)
		}(j)
	}
}

func (w *Writer) delegateJobs(jobs chan<- Job, records <-chan Record) {
	jID := 1
	for r := range records {
		j := Job{
			ID:     jID,
			Record: r,
		}
		jobs <- j

		jID++
	}
}
