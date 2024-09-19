package concurrentreader

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type result struct {
	Min          float64
	Mean         float64
	Max          float64
	NumOfEntries int
	Total        float64
}

func Read(f io.Reader) {
	concurrent(f, 20, 1000000)
}

type CityMap = map[string]result

func concurrent(f io.Reader, numWorkers, batchSize int) {
	cityMap := make(CityMap, 4000)
	// mx := sync.RWMutex{}

	// reader creates and returns a channel that recieves
	// batches of rows (of length batchSize) from the file
	reader := func(ctx context.Context, rowsBatch *[]string) <-chan []string {
		out := make(chan []string)

		scanner := bufio.NewScanner(f)

		go func() {
			defer close(out) // close channel when we are done sending all rows

			for {
				scanned := scanner.Scan()

				select {
				case <-ctx.Done():
					return
				default:
					row := scanner.Text()
					// if batch size is complete or end of file, send batch out
					if len(*rowsBatch) == batchSize || !scanned {
						out <- *rowsBatch
						*rowsBatch = []string{} // clear batch
					}
					*rowsBatch = append(*rowsBatch, row) // add row to current batch
				}

				// if nothing else to scan return
				if !scanned {
					return
				}
			}
		}()

		return out
	}

	// worker takes in a read-only channel to recieve batches of rows.
	// After it processes each row-batch it sends out the processed output
	// on its channel.
	worker := func(ctx context.Context, rowBatch <-chan []string) <-chan CityMap {
		out := make(chan CityMap)

		go func() {
			defer close(out)
			// compile to batch into a processed map
			p := make(CityMap, batchSize)
			for rowBatch := range rowBatch {
				for _, row := range rowBatch {
					c, t := processRow(row)
					pt, err := strconv.ParseFloat(t, 32)

					if err != nil {
						panic(errors.New("error parsing float"))
					}
					val, ok := p[c]
					if !ok {
						p[c] = result{
							Min:          pt,
							Max:          pt,
							Mean:         pt,
							Total:        pt,
							NumOfEntries: 1,
						}
					} else {
						if val.Max < pt {
							val.Max = pt
						}

						if val.Min > pt {
							val.Min = pt
						}

						val.NumOfEntries++

						val.Total += pt

						p[c] = val
					}
				}
			}
			out <- p
		}()

		return out
	}

	// combiner takes in multiple read-only channels that receive processed output
	// (from workers) and sends it out on it's own channel via a multiplexer.
	combiner := func(ctx context.Context, inputs ...<-chan CityMap) <-chan CityMap {
		out := make(chan CityMap)

		var wg sync.WaitGroup
		multiplexer := func(p <-chan CityMap) {
			defer wg.Done()

			for in := range p {
				select {
				case <-ctx.Done():
				case out <- in:
				}
			}
		}

		// add length of input channels to be consumed by mutiplexer
		wg.Add(len(inputs))
		for _, in := range inputs {
			go multiplexer(in)
		}

		// close channel after all inputs channels are closed
		go func() {
			wg.Wait()
			close(out)
		}()

		return out
	}

	// create a main context, and call cancel at the end, to ensure all our
	// goroutines exit without leaving leaks.
	// Particularly, if this function becomes part of a program with
	// a longer lifetime than this function.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// STAGE 1: start reader
	rowsBatch := []string{}
	rowsCh := reader(ctx, &rowsBatch)

	// STAGE 2: create a slice of processed output channels with size of numWorkers
	// and assign each slot with the out channel from each worker.
	workersCh := make([]<-chan CityMap, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workersCh[i] = worker(ctx, rowsCh)
	}

	// STAGE 3: read from the combined channel and calculate the final result.
	// this will end once all channels from workers are closed!
	for processed := range combiner(ctx, workersCh...) {
		// add number of rows processed by worker

		for c, innerVal := range processed {
			val, ok := cityMap[c]
			if !ok {
				cityMap[c] = innerVal
			} else {
				if val.Max < innerVal.Max {
					val.Max = innerVal.Max
				}

				if val.Min > innerVal.Min {
					val.Min = innerVal.Min
				}

				val.NumOfEntries += innerVal.NumOfEntries

				val.Total += innerVal.Total

				val.Mean = val.Total / float64(val.NumOfEntries)
				cityMap[c] = val
			}
		}
	}

	// output result here

	mk := make([]string, len(cityMap))

	i := 0
	for k := range cityMap {
		mk[i] = k
		i++
	}

	sort.Strings(mk)

	for _, key := range mk {
		fmt.Printf("%s;%v;%v;%v\n", key, toFixed(cityMap[key].Min, 1), toFixed(cityMap[key].Mean, 1), toFixed(cityMap[key].Max, 1))
	}
}

// processRow takes a pipe-separated line and returns the firstName, fullName, and month.
// this function was created to be somewhat compute intensive and not accurate.
func processRow(text string) (cityname, tempreture string) {
	row := strings.Split(text, ";")

	return row[0], row[1]
}

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}
