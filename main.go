package main

import (
	"errors"
	"fmt"
	"os"
	"submission/concurrentreader"
	"time"
)

const _fileLocation = "../strictly-come-coding/measurements.txt"

func main() {
	start := time.Now()
	f, err := os.Open(_fileLocation)
	if err != nil {
		panic(errors.New("error reading file"))
	}

	defer f.Close()

	concurrentreader.Read(f)

	elapsed := time.Since(start)
	fmt.Printf("Run took %s", elapsed)
}
