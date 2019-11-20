package batchcsv

import (
	"log"
	"sync"

	rl "go.uber.org/ratelimit"
)

type Pipe struct {
	cfg     BatchConfig
	rl      rl.Limiter
	process Processor
}

type Processor func(in Any) (out Any, err error)

// NewPipe creates a processor pipe
func NewPipe(cfg BatchConfig, p Processor) *Pipe {
	return &Pipe{
		cfg:     cfg,
		rl:      rl.New(cfg.RateLimit),
		process: p,
	}
}

// Process processes an input channel and outputs it
func (p *Pipe) Process(in chan Any) (out chan Any) {
	var wg sync.WaitGroup

	out = make(chan Any, p.cfg.OutputBuffer)
	jobs := make(chan Job, p.cfg.MaxJobs)

	// Delegate processor to another goroutine
	go func(jobs chan Job, in chan Any, out chan Any) {
		// spawn a number of workers
		for i := 1; i <= p.cfg.MaxWorkers; i++ {
			go p.work(jobs, out, &wg)
		}

		// queue jobs
		p.delegateJobs(jobs, in)
		wg.Wait()

		close(jobs)
		close(out)
	}(jobs, in, out)

	return
}

func (p *Pipe) work(jobs <-chan Job, out chan<- Any, wg *sync.WaitGroup) {
	for j := range jobs {
		func(j Job) {
			defer wg.Done()
			wg.Add(1)

			res, err := p.process(j.Payload)
			if err != nil {
				log.Println("failed to process input:", err)
				return
			}
			out <- res
		}(j)
	}
}

func (p *Pipe) delegateJobs(jobs chan<- Job, in <-chan Any) {
	jID := 1
	for d := range in {
		p.rl.Take()

		j := Job{
			ID:      jID,
			Payload: d,
		}
		jobs <- j

		jID++
	}
}
