package reader

import (
	"bufio"
	"fmt"
	"io"
	_ "net/http/pprof"
	"sort"
	"submission/async"
	"submission/model"
	"sync"
)

func Read(f io.Reader) {
	batchLimit := 1000000
	lines := reader(f, batchLimit)
	wg := &sync.WaitGroup{}

	workerLimit := 5
	wg.Add(workerLimit)
	resultsChan := make(chan map[string]*model.Stats)
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

	for _, key := range cities {
		city := cityMap[key]
		mean := (float64(city.Total) / 10) / float64(city.NumOfEntries)
		fmt.Printf("%s;%.1f;%.1f;%.1f\n", key, float64(city.Min)/10, mean, float64(city.Max)/10)
	}
}

func reader(f io.Reader, batchLimit int) <-chan [][]byte {
	scanner := bufio.NewScanner(f)
	out := make(chan [][]byte)

	go func() {
		batch := make([][]byte, batchLimit)
		index := 0

		for scanner.Scan() {
			row := scanner.Bytes()

			b := append([]byte(nil), row...)
			batch[index] = b
			index++

			if index == batchLimit {
				out <- batch
				batch = make([][]byte, batchLimit)
				index = 0
			}
		}

		out <- batch
		close(out)
	}()

	return out
}
