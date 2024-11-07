package main

import (
  "bufio"
	"fmt"
	"net"
	"os"
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
      continue
    }

    go handleConnection(conn)
  }
}

func handleConnection(conn net.Conn) {
  defer conn.Close()
  reader := bufio.NewReader(conn)

  for {
    message, err := reader.ReadString('\n')
    if err != nil {
      fmt.Println("Connection closed")
      return
    }

    message = strings.TrimSpace(message)
    if message == "PING" {
			conn.Write([]byte("+PONG\r\n"))
		}
  }
}
