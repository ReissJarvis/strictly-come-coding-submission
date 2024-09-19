package processor

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
)

func ProcessRow(text string) (city string, temp string) {
	// seperated with ;
	row := strings.Split(text, ";")

	return row[0], row[1]
}

func ProcessBatch(f io.Reader, outputChan chan []string, finished chan bool) {
	//sync pools to reuse the memory and decrease the preassure on //Garbage Collector
	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, 500*1024)
		return lines
	}}
	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}
	slicePool := sync.Pool{New: func() interface{} {
		lines := make([]string, 100)
		return lines
	}}
	r := bufio.NewReader(f)
	var wg sync.WaitGroup //wait group to keep track off all threads
	for {

		buf := linesPool.Get().([]byte)
		n, err := r.Read(buf)
		buf = buf[:n]
		if n == 0 {
			if err != nil {
				fmt.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
		}
		nextUntillNewline, err := r.ReadBytes('\n') //read entire line

		if err != io.EOF {
			buf = append(buf, nextUntillNewline...)
		}

		wg.Add(1)
		go func() {

			//process each chunk concurrently
			//start -> log start time, end -> log end time
			ProcessChunk(buf, &linesPool, &stringPool, &slicePool, outputChan)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println("finished batch processing")
	finished <- true
}

func ProcessChunk(chunk []byte, linesPool *sync.Pool, stringPool *sync.Pool, slicePool *sync.Pool, outputChan chan []string) {
	//another wait group to process every chunk further
	var wg2 sync.WaitGroup
	cities := stringPool.Get().(string)
	cities = string(chunk)
	linesPool.Put(chunk) //put back the chunk in pool
	//split the string by "\n", so that we have slice of logs
	citiesSlice := strings.Split(cities, "\n")

	stringPool.Put(cities)
	chunkSize := 100 //process the bunch of 100 logs in thread
	n := len(citiesSlice)
	noOfThread := n / chunkSize
	if n%chunkSize != 0 { //check for overflow
		noOfThread++
	}
	length := len(citiesSlice)
	//traverse the chunk
	for i := 0; i < length; i += chunkSize {

		wg2.Add(1)
		//process each chunk in saperate chunk
		go func(s int, e int) {
			for i := s; i < e; i++ {
				text := citiesSlice[i]
				if len(text) == 0 {
					continue
				}

				cityParts := strings.SplitN(text, ";", 2)
				_ = cityParts[0]
				_ = cityParts[1]

				// outputChan <- []string{cityName, temp}
			}
			wg2.Done()

		}(i*chunkSize, int(math.Min(float64((i+1)*chunkSize), float64(len(citiesSlice)))))
		//passing the indexes for processing
	}
	wg2.Wait() //wait for a chunk to finish
	citiesSlice = nil
}
