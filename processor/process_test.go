package processor_test

import (
	"submission/processor"
	"testing"
)

func TestProcess(t *testing.T) {

	t.Run("Positive Float", func(t *testing.T) {
		input := []byte("London;12.5")

		city, temp, _ := processor.Process(input)

		if city != "London" {
			t.Errorf("Expected: London, Actual: %s", city)
		}

		if temp != 125 {
			t.Errorf("Expected: 12.5, Actual: %d", temp)
		}
	})

	t.Run("Positive Float", func(t *testing.T) {
		input := []byte("London;12.5")

		city, temp, _ := processor.Process(input)

		if city != "London" {
			t.Errorf("Expected: London, Actual: %s", city)
		}

		if temp != 125 {
			t.Errorf("Expected: 12.5, Actual: %d", temp)
		}
	})

	t.Run("Single DigitPositive Float", func(t *testing.T) {
		input := []byte("London;2.5")

		city, temp, _ := processor.Process(input)

		if city != "London" {
			t.Errorf("Expected: London, Actual: %s", city)
		}

		if temp != 25 {
			t.Errorf("Expected: 2.5, Actual: %d", temp)
		}
	})

	t.Run("Negative Float", func(t *testing.T) {
		input := []byte("London;-12.5")

		city, temp, _ := processor.Process(input)

		if city != "London" {
			t.Fail()
		}

		if temp != -125 {
			t.Fail()
		}
	})

}
