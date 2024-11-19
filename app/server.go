package main

import (
  "bufio"
	"fmt"
	"net"
	"os"
  "strconv"
  "strings"
  "sync"
  "time"
)

type Store struct {
  data      map[string]string
  expiries  map[string]time.Time
  mu        sync.RWMutex
}

func NewStore() *Store {
  return &Store{
    data: make(map[string]string),
    expiries: make(map[string]time.Time),
  }
}

func (s *Store) Set(key, value string, expiry int) {
  s.mu.Lock()
  defer s.mu.Unlock()
  s.data[key] = value
  if expiry > 0 {
    s.expiries[key] = time.Now().Add(time.Duration(expiry) * time.Millisecond)
    go s.expireKeyAfter(key, expiry)
  }
}

func (s *Store) Get(key string) (string, bool) {
  s.mu.RLock()
  defer s.mu.RUnlock()

  if expiry, exists := s.expiries[key]; exists {
    if time.Now().After(expiry) {
      delete(s.data, key)
      delete(s.expiries, key)
      return "", false
    }
  }
  value, exists := s.data[key]
  return value, exists
}

const (
	opCodeModuleAux    byte = 247 /* Module auxiliary data. */
	opCodeIdle         byte = 248 /* LRU idle time. */
	opCodeFreq         byte = 249 /* LFU frequency. */
	opCodeAux          byte = 250 /* RDB aux field. */
	opCodeResizeDB     byte = 251 /* Hash table resize hint. */
	opCodeExpireTimeMs byte = 252 /* Expire time in milliseconds. */
	opCodeExpireTime   byte = 253 /* Old expire time in seconds. */
	opCodeSelectDB     byte = 254 /* DB number of the following keys. */
	opCodeEOF          byte = 255
)

func sliceIndex(data []byte, sep byte) int {
	for i, b := range data {
		if b == sep {
			return i
		}
	}
	return -1
}

func parseTable(bytes []byte) []byte {
	start := sliceIndex(bytes, opCodeResizeDB)
	end := sliceIndex(bytes, opCodeEOF)
	return bytes[start+1 : end]
}
func readFile(path string) string {
	c, _ := os.ReadFile(path)
	key := parseTable(c)
	str := key[4 : 4+key[3]]
	return string(str)
}

func (s *Store) expireKeyAfter(key string, expiry int) {
  time.Sleep(time.Duration(expiry) * time.Millisecond)
  s.mu.Lock()
  defer s.mu.Unlock()
  if expiration, exists := s.expiries[key]; exists && time.Now().After(expiration) {
    delete(s.data, key)
    delete(s.expiries, key)
  }
}

func (s *Store) Del(key string) {
  s.mu.RLock()
  defer s.mu.RUnlock()
  delete(s.data, key)
  delete(s.expiries, key)
}

func main() {
  s := NewStore()
  l, err := net.Listen("tcp", "0.0.0.0:6379")
  if err != nil {
    fmt.Println("Failed to bind to port 6379")
    os.Exit(1)
  }
  defer l.Close()

  for {
    conn, err := l.Accept()
    if err != nil {
      fmt.Println("Error accepting connection: ", err.Error())
      os.Exit(1)
    }

    go handleConnection(conn, s)
  }
}

func handleConnection(conn net.Conn, s *Store) {
  dir := ""
  dbfilename := ""

  args := os.Args
  for i := 1; i < len(args); i++ {
    switch args[i] {
      case "--dir":
        dir = args[i+1]
        i++
      case "--dbfilename":
        dbfilename = args[i+1]
        i++
    }
  }
  defer conn.Close()
  reader := bufio.NewReader(conn)

  for {
    command, err := parseRESP(reader)
    if err != nil {
      fmt.Println("Error parsing RESP: ", err.Error())
      return
    }

    if len(command) < 1 {
      continue
    }

    switch strings.ToUpper(command[0]) {
      case "PING":
        conn.Write([]byte("+PONG\r\n"))
      case "ECHO":
        conn.Write([]byte("+" + command[1] + "\r\n"))
      case "SET":
        key, value := command[1], command[2]
        var expiry int
        if len(command) > 3 && strings.ToUpper(command[3]) == "PX" {
          if exp, err := strconv.Atoi(command[4]); err == nil {
            expiry = exp
          }
        }
        s.Set(key, value, expiry)
        conn.Write([]byte("+OK\r\n"))
      case "GET":
        key := command[1]
        value, exists := s.Get(key)
        if !exists {
          conn.Write([]byte("$-1\r\n"))
        } else {
          conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
        }
      case "CONFIG":
        if command[1] == "GET" {
          switch command[2] {
            case "dir":
              conn.Write([]byte(ToRESP([]string{"dir", dir})))
            case "dbfilename":
              conn.Write([]byte(ToRESP(dbfilename)))
            default:
              conn.Write([]byte("$-1\r\n"))
          }
        }
      case "KEYS":
        if command[1] == "*" {
          fileContent := readFile(dir + "/" + dbfilename)
          conn.Write([]byte(ToRESP([]string{fileContent})))
        }
      default:
        conn.Write([]byte("ERROR: unknown command\n"))
    }
  }
}

func ToRESP(output interface{}) string {
	switch v := output.(type) {
	case string:
		// Simple string or bulk string
		if strings.Contains(v, "\n") || strings.Contains(v, "\r") {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(v), v) // Bulk String
		}
		return fmt.Sprintf("+%s\r\n", v) // Simple String
	case error:
		// Error message
		return fmt.Sprintf("-%s\r\n", v.Error())
	case int, int64:
		// Integer
		return fmt.Sprintf(":%d\r\n", v)
	case []string:
		// Array of strings
		var builder strings.Builder
		builder.WriteString(fmt.Sprintf("*%d\r\n", len(v)))
		for _, element := range v {
			builder.WriteString(ToRESP(element))
		}
		return builder.String()
	case nil:
		// Null bulk string
		return "$-1\r\n"
	default:
		// Unsupported type
		return "-ERR unknown type\r\n"
	}
}

func parseRESP(reader *bufio.Reader) ([]string, error) {
  line, err := reader.ReadString('\n')
  if err != nil {
    return nil, err
  }
  line = strings.TrimSpace(line)

  if len(line) == 0 || line[0] != '*' {
    return nil, fmt.Errorf("invalid RESP format: expected '*' at the start")
  }

  numElements, err := strconv.Atoi(line[1:])
  if err != nil {
    return nil, fmt.Errorf("invalid number of elements: %s", line[1:])
  }

  command := make([]string, 0, numElements)

  for i := 0; i < numElements; i++ {
    line, err := reader.ReadString('\n')
    if err != nil {
      return nil, err
    }
    if line[0] != '$' {
      return nil, fmt.Errorf("invalid bulk string header, expected '$'")
    }

    strLen, err := strconv.Atoi(strings.TrimSpace(line[1:]))
    if err != nil {
      return nil, fmt.Errorf("invalid bulk string length: %s", line[1:])
    }

    str := make([]byte, strLen)
    _, err = reader.Read(str)
    if err != nil {
      return nil, err
    }

    _, err = reader.Discard(2)
    if err != nil {
      return nil, err
    }

    command = append(command, string(str))
  }

  return command, nil
}
