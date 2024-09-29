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
	resultsChan := make(chan []model.Processed)
	for i := 0; i < workerLimit; i++ {
		go async.Worker(lines, batchLimit, resultsChan, wg)
	}
	cityMap := make(map[string]*model.Stats)

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	for processedCities := range resultsChan {
		for _, processed := range processedCities {
			key := processed.City
			temp := processed.Temp
			val, ok := cityMap[key]
			if !ok {
				cityMap[key] = &model.Stats{
					Min:          temp,
					Max:          temp,
					Total:        temp,
					NumOfEntries: 0,
				}
			} else {
				if val.Max < temp {
					val.Max = temp
				}

				if val.Min > temp {
					val.Min = temp
				}

				val.NumOfEntries++

				val.Total += temp

				cityMap[key] = val
			}
		}
	}

	mk := make([]string, len(cityMap))

	i := 0
	for k := range cityMap {
		mk[i] = k
		i++
	}

	sort.Strings(mk)

	for _, key := range mk {
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
