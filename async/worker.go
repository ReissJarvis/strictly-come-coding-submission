package async

import (
	"submission/model"
	"submission/processor"
	"sync"
)

func Worker(jobs <-chan [][]byte, batchLimit int, results chan<- []model.Processed, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := range jobs {
		processed := make([]model.Processed, batchLimit)

		for i, row := range j {
			city, temp, valid := processor.Process(row)

			if !valid {
				continue
			}

			processed[i] = model.Processed{
				City: city,
				Temp: temp,
			}
		}

		results <- processed
	}
}
