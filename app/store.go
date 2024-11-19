package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

const (
	opCodeTypeString byte = 0x00 // following byte(s) are length encoding
	opCodeAuxField   byte = 0xFA // key, value follow
	opCodeResizeDB   byte = 0xFB // follwing are 2 length-encoded ints
	opCodeExpMilSec  byte = 0xFC // following 8 bytes are expration time (ms)
	opCodeExpSec     byte = 0xFD // following 4 bytes are expration time (s)
	opCodeSelectDB   byte = 0xFE // following byte is db number
	opCodeEOF        byte = 0xFF // following 8 bytes are CRC64 checksum
)

type Store struct {
	kv     map[string]string
	expiry map[string]time.Time
	db     *os.File
}

func NewStore() *Store {
	kv := make(map[string]string)
	exp := make(map[string]time.Time)
	return &Store{kv: kv, expiry: exp}
}

// Load loads the in-memory KV map with values from the db.
func (s *Store) Load(db *os.File) error {
	if db != nil {
		s.db = db
	}
	defer db.Close()

	data, err := parseRDB(db, s.expiry)
	if err != nil {
		return err
	}

	for i := 0; i < len(data); i += 2 {
		if err := s.Add(data[i], data[i+1]); err != nil {
			return err
		}
	}

	return nil
}

// Save stores the in-memory KV map to the db.
func (s *Store) Save() error {
	return nil
}

// Get retreives the value for the given key from the KV map. An error
// is returned if the key is not found. A key of "*" will return an encoded
// array of all keys.
func (s *Store) Get(key string) (string, error) {
	if key == "*" {
		keys := []string{}
		for key := range s.kv {
			keys = append(keys, key)
		}
		return encodeBulkStringArray(len(keys), keys...), nil
	}

	val, found := s.kv[key]
	if !found {
		return "", fmt.Errorf("key %q not found", key)
	}
	return val, nil
}

// Add stores the KV-pair in the KV map. An error will be returned if the
// key already exists.
func (s *Store) Add(key, val string) error {
	_, found := s.kv[key]
	if found {
		return fmt.Errorf("key %q already exists", key)
	}
	s.kv[key] = val
	return nil
}

// Update replaces the value of an existing key to a new one. An error is
// returned if the key is not found.
func (s *Store) Update(key, val string) error {
	_, found := s.kv[key]
	if !found {
		return fmt.Errorf("key %q not found", key)
	}
	s.kv[key] = val
	return nil
}

// Delete removes the given key and its value from the KV map. An error
// is returned if the key is not found.
func (s *Store) Delete(key string) error {
	_, found := s.kv[key]
	if !found {
		return fmt.Errorf("key %q not found", key)
	}
	delete(s.kv, key)
	return nil
}

// parseRDB parses the values in the RDB file.
func parseRDB(file *os.File, expiry map[string]time.Time) ([]string, error) {
	reader := bufio.NewReader(file)
	result := []string{}

	// Read header.
	header := make([]byte, 9)
	_, err := reader.Read(header)
	if err != nil {
		return result, err
	}
	fmt.Printf("RDB file header: %s %s", header[:5], header[5:])

	// Skip the junk after the header.
	if _, err := reader.ReadBytes(opCodeResizeDB); err != nil {
		return result, err
	}
	if _, err := reader.Read(make([]byte, 2)); err != nil {
		return result, err
	}

	// Read in the rest of the data.
	var expiration time.Time
	var hasExpiration bool
	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			return result, err
		}

		switch opcode {
		case opCodeSelectDB:
			// Follwing byte(s) is the db number.
			dbNum, err := decodeLength(reader)
			if err != nil {
				return result, err
			}
			fmt.Printf("DB number: %d", dbNum)
		case opCodeAuxField:
			// Length prefixed key and value strings follow.
			kv := [][]byte{}
			for i := 0; i < 2; i++ {
				length, err := decodeLength(reader)
				if err != nil {
					return result, err
				}
				data := make([]byte, int(length))
				if _, err = reader.Read(data); err != nil {
					return result, err
				}
				kv = append(kv, data)
			}
			fmt.Printf("AUX key-value pair: %s: %s", kv[0], kv[1])
		case opCodeResizeDB:
			// Implement
		case opCodeExpSec:
			data := make([]byte, 4)
			if _, err = reader.Read(data); err != nil {
				return result, err
			}
			timestamp := binary.LittleEndian.Uint32(data)
			expiration = time.Unix(int64(timestamp), 0).UTC()
			hasExpiration = true
			fmt.Printf("Expiration %s", expiration.String())
		case opCodeExpMilSec:
			data := make([]byte, 8)
			if _, err = reader.Read(data); err != nil {
				return result, err
			}
			timestamp := binary.LittleEndian.Uint64(data)
			expiration = time.UnixMilli(int64(timestamp)).UTC()
			hasExpiration = true
			fmt.Printf("Expiration %s", expiration.String())
		case opCodeTypeString:
			kv := [][]byte{}
			for i := 0; i < 2; i++ {
				length, err := decodeLength(reader)
				if err != nil {
					return result, err
				}
				data := make([]byte, int(length))
				if _, err = reader.Read(data); err != nil {
					return result, err
				}
				kv = append(kv, data)
			}

			if hasExpiration {
				hasExpiration = false
				if expiration.Before(time.Now().UTC()) {
					hasExpiration = false
					break
				}
				expiry[string(kv[0])] = expiration
			}

			result = append(result, string(kv[0]), string(kv[1]))
			fmt.Printf("STRING key-value pair: %s: %s", kv[0], kv[1])
		case opCodeEOF:
			// Get the 8-byte checksum after this
			checksum := make([]byte, 8)
			_, err := reader.Read(checksum)
			if err != nil {
				return result, err
			}
			fmt.Printf("Checksum: %s", hex.EncodeToString(checksum))
			return result, nil
		default:
			// Handle any other tags.
		}
	}
}

func decodeLength(r *bufio.Reader) (int, error) {
	num, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch {
	case num <= 63: // leading bits 00
		// Remaining 6 bits are the length.
		return int(num & 0b00111111), nil
	case num <= 127: // leading bits 01
		// Remaining 6 bits plus next byte are the length
		nextNum, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint16([]byte{num & 0b00111111, nextNum})
		return int(length), nil
	case num <= 191: // leading bits 10
		// Next 4 bytes are the length
		bytes := make([]byte, 4)
		_, err := r.Read(bytes)
		if err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint32(bytes)
		return int(length), nil
	case num <= 255: // leading bits 11
		// Next 6 bits indicate the format of the encoded object.
		// TODO: This will result in problems on the next read, possibly.
		valueType := num & 0b00111111
		return int(valueType), nil
	default:
		return 0, err
	}
}
