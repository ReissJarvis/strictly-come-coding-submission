package main

import (
	"errors"
	"os"
	"submission/reader"
)

const _fileLocation = "../strictly-come-coding/measurements.txt"

func main() {
	fileLocation := _fileLocation

	if len(os.Args) > 1 {
		fileLocation = os.Args[1]
	}

	f, err := os.Open(fileLocation)
	if err != nil {
		panic(errors.New("error reading file"))
	}

	defer f.Close()

	reader.Read(f)
}
