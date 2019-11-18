package batchcsv

// BatchConfig
type BatchConfig struct {
	MaxJobs    int
	MaxWorkers int
	RateLimit  int
}

// Job represents a batch job
type Job struct {
	ID     int
	Record []string
}

// Record wraps a CSV record
type Record []string