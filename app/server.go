package main

import (
	"context"
	"fmt"
	"net"
  "os"
	"os/signal"
  "strings"
	"sync"
	"syscall"
  "time"
)

type Server struct {
	Context context.Context
	Config  *Store
}

func NewServer(ctx context.Context, config *Store) *Server {
	server := &Server{Context: ctx, Config: config}
	return server
}

func (s *Server) Run() error {

	// Listen on Context for SIGINT or SIGTERM to shutdown gracefully.
	ctx, stop := signal.NotifyContext(s.Context, syscall.SIGINT, syscall.SIGTERM)
	defer stop() // stop will trigger ctx.Done() signal

	var wg sync.WaitGroup

	// Start TCP listener.
	host, _ := s.Config.Get(keyHost)
	port, _ := s.Config.Get(keyPort)
  master, _ := s.Config.Get(replicaOf)

  if master != "" {
    params := strings.Split(master, " ")
    address := strings.Join(params, ":")
    m, err := net.Dial("tcp", address)
    if err != nil {
      return fmt.Errorf("failed to connect to master: %v", err)
    }
    m.Write([]byte("*1\r\n$4\r\nPING\r\n"))
    time.Sleep(1 * time.Second)
		m.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
		time.Sleep(1 * time.Second)
		m.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
		time.Sleep(1 * time.Second)
    m.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
  }
  args := os.Args
  for i := 1; i < len(args); i++ {
    switch args[i] {
    case "--port":
      port = args[i+1]
      i++;
    }
  }
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %v", port, err)
	}
	fmt.Printf("Listening on port %s...", port)

	// Start goroutine that stops listener when signal is received.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		fmt.Printf("Closing listener...")
		listener.Close()
	}()

	// Listen for client connections and send to handler.
	for {
		conn, err := listener.Accept()
		if err != nil {
			// Accept will return error when the listener is closed.  Use
			// this to implement graceful shutdown.
			fmt.Printf("Shutting down server...")

			// Send ctx.Done() to handlers. TODO: this doesn't work!
			stop()
			fmt.Printf("Stop message sent, waiting for goroutines to complete...")

			// Wait for goroutines to complete.
			wg.Wait()
			fmt.Printf("Waitgroup clear.")

			switch err.(type) {
			case *net.OpError:
				// net.OpError is received when listener is closed. No
				// need to return this error, it is expected on shutdown.
				return nil
			default:
				// Any other error type gets sent back to caller.
				return err
			}
		}

		// Create ClientHandler and start goroutine.
		client := NewClientHandler(ctx, conn, s)
		wg.Add(1)
		go client.Handle(&wg)

		fmt.Printf("Client connected.")
	}
}

// IsPersistent returns true if a db directory and filename are defined.
func (s *Server) IsPersistent() bool {
	_, dirErr := s.Config.Get(keyDBDir)
	_, fileErr := s.Config.Get(keyDBFilename)
	return dirErr == nil && fileErr == nil
}
