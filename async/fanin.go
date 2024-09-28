package async

import (
	"submission/model"
	"sync"
)

func FanIn(inps ...<-chan []model.Processed) <-chan []model.Processed {
	wg := sync.WaitGroup{}

	out := make(chan []model.Processed)

	output := func(c <-chan []model.Processed) {
		for n := range c {
			out <- n
		}

		wg.Done()
	}

	wg.Add(len(inps))
	for _, c := range inps {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
