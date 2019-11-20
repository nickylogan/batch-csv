package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/nickylogan/batch-csv/batchcsv"
)

func main() {
	rand.Seed(time.Now().Unix())

	cfg := batchcsv.BatchConfig{
		MaxJobs:      100,
		MaxWorkers:   100,
		RateLimit:    100,
		OutputBuffer: 100,
	}

	// Create batch reader
	r, err := batchcsv.NewReader("data.csv", cfg)
	if err != nil {
		log.Panic(err)
	}
	defer r.Close()

	// Create process pipe
	logger := func(in batchcsv.Any) (out batchcsv.Any, err error) {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(5000)))
		fmt.Println("finished:", in)
		return in, nil
	}
	p := batchcsv.NewPipe(cfg, logger)

	// Create duplicator
	d := batchcsv.NewDuplicator(cfg)

	// Create batch writer
	parser := func(a batchcsv.Any) (rec batchcsv.Record, err error) {
		rec, ok := a.([]string)
		if !ok {
			return nil, errors.New("can't parse payload")
		}
		return rec, nil
	}
	w1, err := batchcsv.NewWriter("result1.csv", cfg, parser)
	if err != nil {
		log.Panic(err)
	}
	defer w1.Close()

	w2, err := batchcsv.NewWriter("result2.csv", cfg, parser)
	if err != nil {
		log.Panic(err)
	}
	defer w2.Close()

	// Start pipeline
	outs := d.Duplicate(p.Process(r.Read()), 2)
	done1 := w1.Write(outs[0])
	done2 := w2.Write(outs[1])
	<-done1
	<-done2
}
