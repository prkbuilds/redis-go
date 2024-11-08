package main

import (
  "bufio"
	"fmt"
	"net"
	"os"
  "strconv"
  "strings"
  "sync"
)

type Store struct {
  data map[string]string
  mu sync.RWMutex
}

func NewStore() *Store {
  return &Store{
    data: make(map[string]string),
  }
}

func (s *Store) Set(key, value string) {
  s.mu.Lock()
  defer s.mu.Unlock()
  s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
  s.mu.RLock()
  defer s.mu.RUnlock()
  value, exists := s.data[key]
  return value, exists
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
        s.Set(key, value)
        conn.Write([]byte("+OK\r\n"))
      case "GET":
        key := command[1]
        value, _ := s.Get(key)
        conn.Write([]byte("+" + value + "\r\n"))
      default:
        conn.Write([]byte("ERROR: unknown command\n"))
    }
  }
}

func parseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("invalid RESP format")
	}

	numElements, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	command := make([]string, 0, numElements)
	for i := 0; i < numElements; i++ {
		line, err := reader.ReadString('\n')
		if err != nil || line[0] != '$' {
			return nil, fmt.Errorf("invalid bulk string")
		}

		strLen, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
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
