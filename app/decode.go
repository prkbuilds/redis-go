package main

import (
	"fmt"
	"strconv"
)

const (
	fmtArray        = "*%d\r\n"
	fmtBulkStr      = "$%d\r\n%s\r\n"
	fmtSimpleString = "+%s\r\n"
)

func decodeArrayLength(msg string) int {
	length, err := strconv.Atoi(msg[1:])
	if err != nil {
		fmt.Printf("Error parsing array length: %v", err)
	}
	return length
}

func encodeSimpleString(str string) string {
	return fmt.Sprintf(fmtSimpleString, str)
}

func encodeBulkString(str string) string {
	return fmt.Sprintf(fmtBulkStr, len(str), str)
}

func encodeBulkStringArray(length int, bulkStrings ...string) string {
	encoded := fmt.Sprintf(fmtArray, length)
	for _, str := range bulkStrings {
		encoded += encodeBulkString(str)
	}
	return encoded
}
