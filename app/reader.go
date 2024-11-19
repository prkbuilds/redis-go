package main

import (
	"fmt"
	"os"
)

func decodeSize(encoded []byte) int {
	if len(encoded) == 0 {
		return 0
	}
	switch encoded[0]&0xC0 {
    case 0x00:
      return int(encoded[0] & 0x3F)
    case 0x40:
      return int(encoded[0]&0x3F)<<8 | int(encoded[1])
    case 0x80:
      return int(encoded[1])<<24 | int(encoded[2])<<16 | int(encoded[3])<<8 | int(encoded[4])
	}
	return 0
}

func readRDBFile(path string, s *Store) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("error reading RDB file: %v", err)
	}

	if string(content[:9]) != "REDIS0011" {
		return fmt.Errorf("invalid RDB header")
	}

	fmt.Println("RDB Header valid")

	idx := 9

  for idx < len(content) {
    switch content[idx] {
      case 0xFA:
        idx++
        nameLen := decodeSize(content[idx : idx+1])
        idx++

        name := string(content[idx : idx+nameLen])
        idx += nameLen

        valueLen := decodeSize(content[idx : idx+1])
        idx++

        value := string(content[idx : idx+valueLen])
        idx += valueLen

        fmt.Printf("Metadata: %s = %s\n", name, value)
      case 0xFE:
        idx++
        idx++
      case 0xFB:
        idx++

        hashTableSize := decodeSize(content[idx : idx+1])
        idx++

        for i := 0; i < hashTableSize; i++ {
          keyLen := decodeSize(content[idx : idx+1])
          idx++

          key := string(content[idx : idx+keyLen])
          idx += keyLen

          valueLen := decodeSize(content[idx : idx+1])
          idx++

          value := string(content[idx : idx+valueLen])
          idx += valueLen

          fmt.Printf("Key: %s, Value: %s\n", key, value)
        }
		  default:
        break
    }
	}

	if content[idx] == 0xFF {
		fmt.Println("End of RDB file reached.")
	}

	return nil
}
