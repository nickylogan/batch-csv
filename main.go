package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/nickylogan/batch-csv/batchcsv"
)

func main() {
	cfg := batchcsv.BatchConfig{
		MaxJobs:    100,
		MaxWorkers: 100,
		RateLimit:  100,
	}

	// Create batch reader
	r, err := batchcsv.NewReader("data.csv", cfg)
	if err != nil {
		log.Panic(err)
	}
	defer r.File.Close()

	// Create batch writer
	w, err := batchcsv.NewWriter("result.csv", cfg)
	if err != nil {
		log.Panic(err)
	}
	defer w.File.Close()

	// Create channel to pipe results from reader to writer
	results := make(chan batchcsv.Record, 100)
	go w.Write(results)

	// Run batch reader
	r.Run(func(j batchcsv.Job) {
		fmt.Printf("%d: started\n", j.ID)
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(2000)))
		results <- j.Record
		fmt.Printf("%d: finished\n", j.ID)
	})
	close(results)
}
