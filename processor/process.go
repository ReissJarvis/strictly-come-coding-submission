package processor

import (
	"bytes"
)

func Process(input []byte) (city string, temp int, isValid bool) {
	cityBytes, tempBytes, hasFound := bytes.Cut(input, []byte(";"))

	if !hasFound {
		return "", 0, false
	}

	i := 0
	negative := false

	if tempBytes[i] == '-' {
		negative = true
		i++
	}

	temp += int(tempBytes[i]-'0') * 10
	i++

	if tempBytes[i] != '.' {
		temp = temp*10 + int(tempBytes[i]-'0')*10
		i++
	}

	i++

	temp += int((tempBytes[i] - '0'))

	if negative {
		temp = -temp
	}

	return string(cityBytes), temp, true
}
