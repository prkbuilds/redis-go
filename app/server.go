package main

import (
	"context"
	"fmt"
	"net"
	"os/signal"
  "strings"
	"sync"
	"syscall"
  "time"
)

type Server struct {
	Context   context.Context
	Config    *Store
  Replicas  []net.Conn
  mu        sync.Mutex
}

func (s *Server) AddReplica(replica net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Replicas = append(s.Replicas, replica)
}

func (s *Server) RemoveReplica(replica net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, r := range s.Replicas {
		if r == replica {
			s.Replicas = append(s.Replicas[:i], s.Replicas[i+1:]...)
			break
		}
	}
}

func (s *Server) GetReplicas() []net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]net.Conn(nil), s.Replicas...) // Return a copy
}

func (s *Server) CloseReplicas() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, replica := range s.Replicas {
    if replica != nil {
  		replica.Close()
    }
	}
	s.Replicas = nil
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

  fmt.Printf("Running server on %s:%s\n", host, port)
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %v", port, err)
	}
	fmt.Printf("Listening on port %s...", port)

  var masterConn net.Conn
  if master != "" {
    params := strings.Split(master, " ")
    address := strings.Join(params, ":")
    conn, err := net.Dial("tcp", address)
    if err != nil {
      return fmt.Errorf("failed to connect to master: %v", err)
    }
    masterConn = conn

    commands := []string{
      "*1\r\n$4\r\nPING\r\n",
      fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(port), port),
      "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
      "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
    }

    for _, cmd := range commands {
      if _, err := masterConn.Write([]byte(cmd)); err != nil {
        return fmt.Errorf("failed to send command to master: %v", err)
      }
      time.Sleep(1 * time.Second)
    }
  }

	// Start goroutine that stops listener when signal is received.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
    s.CloseReplicas()
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
