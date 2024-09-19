package batchreader

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	"submission/processor"
	"sync"
)

func Read(f io.Reader) {
	lines := make(chan string)
	//cache := make(map[string][]string)

	// start four workers to do the heavy lifting
	wc1 := startWorker(lines)
	wc2 := startWorker(lines)
	wc3 := startWorker(lines)
	wc4 := startWorker(lines)
	scanner := bufio.NewScanner(f)
	go func() {
		defer close(lines)
		for scanner.Scan() {
			lines <- scanner.Text()
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}()

	merged := merge(wc1, wc2, wc3, wc4)
	for line := range merged {
		fmt.Printf("%s;%v;%v;%v\n", line[0], toFixed(line[1], 1), toFixed(line[2], 1), toFixed(line[3], 1))
	}
}

func startWorker(lines <-chan string) <-chan []string {
	finished := make(chan []string)
	go func() {
		defer close(finished)
		for line := range lines {
			// Do your heavy work here
			city, temp := processor.ProcessRow(line)

			finished <- []string{city, temp}
		}
	}()
	return finished
}

func merge(cs ...<-chan []string) <-chan []string {
	var wg sync.WaitGroup
	out := make(chan []string)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan []string) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func toFixed(strnum string, precision int) float64 {
	num, err := strconv.ParseFloat(strnum, 64)
	if err != nil {
		panic("unable to parse float, bad float in file")
	}

	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}
