package sequentialreader

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"submission/processor"
)

type tempCollated struct {
	Min          float64
	Mean         float64
	Max          float64
	NumOfEntries int
	Total        float64
}

func Read(f io.Reader) {
	cityMap := make(map[string]tempCollated)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		row := scanner.Text()

		city, temp := processor.ProcessRow(row)
		parsedTemp, err := strconv.ParseFloat(temp, 32)

		if err != nil {
			panic(errors.New("error parsing float"))
		}

		val, ok := cityMap[city]
		if !ok {
			cityMap[city] = tempCollated{
				Min:          parsedTemp,
				Max:          parsedTemp,
				Mean:         parsedTemp,
				Total:        parsedTemp,
				NumOfEntries: 1,
			}
		} else {
			if val.Max < parsedTemp {
				val.Max = parsedTemp
			}

			if val.Min > parsedTemp {
				val.Min = parsedTemp
			}

			val.NumOfEntries++

			val.Total += parsedTemp

			val.Mean = val.Total / float64(val.NumOfEntries)
			cityMap[city] = val
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
		fmt.Printf("%s;%v;%v;%v\n", key, toFixed(cityMap[key].Min, 1), toFixed(cityMap[key].Mean, 1), toFixed(cityMap[key].Max, 1))
	}
}

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}
