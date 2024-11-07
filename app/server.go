package main

import (
  "bufio"
	"fmt"
	"net"
	"os"
  "strconv"
  "strings"
)

func main() {
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

    go handleConnection(conn)
  }
}

func handleConnection(conn net.Conn) {
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
