package reader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"submission/async"
	"submission/model"
	"sync"
)

func Read(f io.Reader) {
	batchLimit := 500000
	lines := reader(f, batchLimit)
	wg := &sync.WaitGroup{}

	workerLimit := runtime.GOMAXPROCS(runtime.NumCPU())

	wg.Add(workerLimit)
	resultsChan := make(chan map[string]*model.Stats, batchLimit)
	for i := 0; i < workerLimit; i++ {
		go async.Worker(lines, batchLimit, resultsChan, wg)
	}
	cityMap := make(map[string]*model.Stats)

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	for processedCities := range resultsChan {
		for key, p := range processedCities {

			val, ok := cityMap[key]
			if !ok {
				cityMap[key] = p
				continue
			}

			val.Max = max(val.Max, p.Max)
			val.Min = min(val.Min, p.Min)
			val.NumOfEntries += p.NumOfEntries
			val.Total += p.Total

			cityMap[key] = val
		}
	}

	cities := make([]string, 0, len(cityMap))
	for k := range cityMap {
		cities = append(cities, k)
	}
	sort.Strings(cities)

	var buffer bytes.Buffer

	for _, key := range cities {
		city := cityMap[key]

		mean := (float64(city.Total) / 10) / float64(city.NumOfEntries)

		buffer.WriteString(key + ";" + strconv.FormatFloat(float64(city.Min)/10, 'f', 1, 64) + ";" + strconv.FormatFloat(mean, 'f', 1, 64) + ";" + strconv.FormatFloat(float64(city.Max)/10, 'f', 1, 64) + "\n")
		//fmt.Printf("%s;%.1f;%.1f;%.1f\n", key, float64(city.Min)/10, mean, float64(city.Max)/10)
	}

	fmt.Print(buffer.String())
}

func reader(f io.Reader, batchLimit int) <-chan [][]byte {
	scanner := bufio.NewScanner(f)
	out := make(chan [][]byte, batchLimit)

	go func() {
		batch := make([][]byte, 0, batchLimit)

		for scanner.Scan() {
			row := scanner.Bytes()
			bytes := append(make([]byte, 0, len(row)), row...)
			batch = append(batch, bytes)

			if len(batch) >= batchLimit {
				out <- batch
				batch = make([][]byte, 0, batchLimit)
			}
		}

		if len(batch) > 0 {
			out <- batch
		}

		close(out)
	}()

	return out
}
