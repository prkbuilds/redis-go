package main

import (
	"context"
	"os"
  "fmt"
)

const (
	flagDBDir      = "--dir"        // command-line flag for db directory
	flagDBFilename = "--dbfilename" // command-line flag for db filename
	replicaOf      = "--replicaof"  // command-line flag for replica of
	keyDBDir       = "dir"          // store key for db directory
	keyDBFilename  = "dbfilename"   // store key for db filename
	keyHost        = "host"         // store key for server host URL
	keyPort        = "port"         // store key for server port
)

func main() {
	ctx := context.Background()
	cfg := NewStore()

	if err := cfg.Add(keyHost, "localhost"); err != nil {
		fmt.Printf(err.Error())
	}

	if err := cfg.Add(keyPort, "6379"); err != nil {
		fmt.Printf(err.Error())
	}

	// Handle command-line arguments.
	args := os.Args
	for i := 1; i < len(args); i += 2 {
		if args[i] == flagDBDir && len(args) >= i {
			if err := cfg.Add(keyDBDir, args[i+1]); err != nil {
				fmt.Printf(err.Error())
			}
		} else if args[i] == flagDBFilename && len(args) >= i {
			if err := cfg.Add(keyDBFilename, args[i+1]); err != nil {
				fmt.Printf(err.Error())
			}
		} else if args[i] == replicaOf && len(args) >= i {
      if err := cfg.Add(replicaOf, args[i+1]); err != nil {
        fmt.Printf(err.Error())
      }
    }
	}
	fmt.Printf("Server config: %+v", cfg)

	// Initiate server.
	s := NewServer(ctx, cfg)
	if err := s.Run(); err != nil {
		fmt.Printf("Server error: ", err)
	}

	fmt.Printf("Server shutdown complete.")
}
