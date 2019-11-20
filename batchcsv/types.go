package batchcsv

// Any type
type Any interface{}

// BatchConfig
type BatchConfig struct {
	MaxJobs      int
	MaxWorkers   int
	RateLimit    int
	OutputBuffer int
}

// Job represents a batch job
type Job struct {
	ID      int
	Payload Any
}

// Record wraps a CSV record
type Record []string
