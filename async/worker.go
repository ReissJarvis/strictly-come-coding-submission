package async

import (
	"submission/model"
	"submission/processor"
	"sync"
)

func Worker(jobs <-chan [][]byte, batchLimit int, results chan<- map[string]*model.Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := range jobs {
		cityMap := make(map[string]*model.Stats)

		for _, row := range j {
			city, temp, valid := processor.Process(row)

			if !valid {
				continue
			}

			val, ok := cityMap[city]
			if !ok {
				cityMap[city] = &model.Stats{
					Min:          temp,
					Max:          temp,
					NumOfEntries: 1,
					Total:        temp,
				}
				continue
			}

			val.Max = max(val.Max, temp)
			val.Min = min(val.Min, temp)
			val.NumOfEntries++
			val.Total += temp

			cityMap[city] = val
		}

		results <- cityMap
	}
}
