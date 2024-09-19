package binaryreader

import (
	"bufio"
	"io"
	"log"
)

func Read(f io.Reader) {
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		_ = scanner.Bytes()
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

}
