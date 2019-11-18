package batchcsv

import (
	"log"
	"os"
	"sync"

	"github.com/smartystreets/scanners/csv"
	"go.uber.org/ratelimit"
)

// Reader wraps batch csv reader
type Reader struct {
	File *os.File
	rl   ratelimit.Limiter
	sc   *csv.Scanner
	cfg  BatchConfig
}

// NewReader creates a new batch reader
func NewReader(fileName string, cfg BatchConfig) (*Reader, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		File: f,
		rl:   ratelimit.New(cfg.RateLimit),
		sc:   csv.NewScanner(f),
		cfg:  cfg,
	}
	return r, nil
}

// Run runs the job handler. Internally, it spawns a limited amount of worker
// to handle the given jobs. The job queue is a buffered channel, with a given
// max amount of jobs at the same time.
func (r *Reader) Run(handler func(j Job)) {
	var wg sync.WaitGroup
	jobs := make(chan Job, r.cfg.MaxJobs)

	// spawn a number of workers
	for w := 1; w <= r.cfg.MaxWorkers; w++ {
		go r.work(jobs, &wg, handler)
	}

	// queue jobs
	r.delegateJobs(jobs)
	wg.Wait()

	close(jobs)
}

func (r *Reader) work(jobs <-chan Job, wg *sync.WaitGroup, handler func(j Job)) {
	for j := range jobs {
		func(j Job) {
			defer wg.Done()
			wg.Add(1)

			handler(j)
		}(j)
	}
}

func (r *Reader) delegateJobs(jobs chan<- Job) {
	jID := 1
	for r.sc.Scan() {
		if err := r.sc.Error(); err != nil {
			log.Panic("error while scanning csv:", err)
		}

		r.rl.Take()
		j := Job{
			ID:     jID,
			Record: r.sc.Record(),
		}
		jobs <- j
		jID++
	}
}
