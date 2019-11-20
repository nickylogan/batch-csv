package batchcsv

// Duplicator is a common class for duplicating
// a data stream
type Duplicator struct {
	cfg BatchConfig
}

// NewDuplicator creates a Duplicator instance
func NewDuplicator(cfg BatchConfig) *Duplicator {
	return &Duplicator{cfg}
}

// Duplicate duplicates the input data stream into several output streams
// based on the given count
func (d *Duplicator) Duplicate(in chan Any, count int) (outs []chan Any) {
	// Create duplicate channels
	outs = make([]chan Any, 0, count)
	for i := 0; i < count; i++ {
		outs = append(outs, make(chan Any))
	}

	// Pipe input to all of the outputs
	go func(outs []chan Any) {
		for x := range in {
			for _, o := range outs {
				o <- x
			}
		}
		for _, o := range outs {
			close(o)
		}
	}(outs)

	return outs
}
