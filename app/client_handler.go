package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	pingResponse = "+PONG\r\n"
	okResponse   = "+OK\r\n"
	nullResponse = "$-1\r\n"
)

type ClientHandler struct {
	Context context.Context
	Conn    io.ReadWriteCloser
	Server  *Server
	Store   *Store
}

func NewClientHandler(ctx context.Context, conn io.ReadWriteCloser, server *Server) *ClientHandler {
	return &ClientHandler{
		Context: ctx,
		Conn:    conn,
		Server:  server,
		Store:   NewStore(),
	}
}

type Command struct {
	Command string
	Args    []string
}

// Handle manages client communication with the server.
func (c *ClientHandler) Handle(wg *sync.WaitGroup) {
	defer c.Conn.Close()
	defer wg.Done()
	fmt.Printf("Connection initiated.")
	scanner := bufio.NewScanner(c.Conn)

	var (
		cmd         Command // Command object
		cmdLength   int     // Number of words in command + args
		cmdReceived int     // Number of words received from client
	)

	if c.Server.IsPersistent() {
		file, err := c.dbFile()
		if err != nil {
			fmt.Errorf("Error opening db file: %v", err)
		}
		if err := c.Store.Load(file); err != nil {
			fmt.Errorf("Error reading from db: %v", err)
		}
	} else {
		fmt.Printf("Database file not provided, data will not be saved between sessions.")
	}

	for {
		select {
		case <-c.Context.Done():
			fmt.Printf("Client handler stopping...")
			return
		default:
			for scanner.Scan() {
				msg := scanner.Text()

				switch msg[0] {
				case '*':
					var length int
					if len(msg) > 1 {
						length = decodeArrayLength(msg)
					} else {
						// "*" can be passed as a parameter for KEYS command.
						length = 0
					}
					cmdLength = length
					cmdReceived = 0
				case '$':
					// Don't need to do anything with this token for now.
				default:
					if cmdReceived < cmdLength {
						if cmdReceived == 0 {
							cmd.Command = msg
						} else {
							cmd.Args = append(cmd.Args, msg)
						}
						cmdReceived++
					}
				}

				// Execute command if the array is complete.
				if cmdReceived == cmdLength {
					if err := c.executeCommand(cmd); err != nil {
						fmt.Errorf("Error executing command %v: %v", cmd, err)
					}
					// Reset for next command.
					cmd = Command{}
					cmdLength = 0
				}
			}
		}
	}
}

// executeCommand executes the command in the command array.
func (c *ClientHandler) executeCommand(cmd Command) error {
	switch cmd.Command {
	case "PING":
		return c.handlePing()
	case "ECHO":
		return c.handleEcho(cmd.Args)
	case "SET":
		return c.handleSet(cmd.Args)
	case "GET":
		return c.handleGet(cmd.Args)
	case "CONFIG":
		return c.handleConfig(cmd.Args)
	case "KEYS":
		return c.handleKeys(cmd.Args)
	case "INFO":
		return c.handleInfo(cmd.Args)
	default:
		return fmt.Errorf("unrecognized command %q", cmd.Command)
	}
}

// handlePing handles PING commands.
func (c *ClientHandler) handlePing() error {
	fmt.Printf("PING command received.")
	return c.send(pingResponse)
}

// handleEcho handles ECHO commands.
func (c *ClientHandler) handleEcho(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("insufficient number of arguments for ECHO")
	}
	valToEcho := args[0]
	fmt.Printf("ECHO %q command received.", valToEcho)
	return c.send(encodeBulkString(valToEcho))
}

// handleSet handles SET commands.
func (c *ClientHandler) handleSet(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("insufficient number of arguments for SET")
	}
	key := args[0]
	value := args[1]
	fmt.Printf("SET %s: %q command received.", key, value)
	if err := c.Store.Add(key, value); err != nil {
		return err
	}

	// Check for expiration arguments.
	if len(args) == 4 && args[2] == "px" {
		expiry, err := strconv.Atoi(args[3])
		if err != nil {
			return fmt.Errorf("error parsing expiration time: %v", err)
		}
		duration := time.Duration(expiry) * time.Millisecond
		c.Store.expiry[key] = time.Now().UTC().Add(duration)
	}
	return c.send(okResponse)
}

// handleGet handles GET commands.
func (c *ClientHandler) handleGet(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("insufficient number of arguments for GET")
	}
	key := args[0]
	fmt.Printf("GET %s command received.", key)
	val, err := c.Store.Get(key)
	if err != nil {
		fmt.Errorf(err.Error())
		return c.send(nullResponse)
	}

	// Check if KV pair is expired. If so, delete and return null.
	expiry, hasExpiration := c.Store.expiry[key]
	if hasExpiration && expiry.Before(time.Now().UTC()) {
		if err := c.Store.Delete(key); err != nil {
			fmt.Errorf(err.Error())
		}
		delete(c.Store.expiry, key) // remove expiry record
		return c.send(nullResponse)
	}

	return c.send(encodeSimpleString(val))
}

// handleConfig handles CONFIG requests.
func (c *ClientHandler) handleConfig(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("insufficient number of arguments for CONFIG")
	}

	subCmd := strings.ToLower(args[0])

	switch subCmd {
	case "get":
		key := args[1]
		fmt.Printf("CONFIG GET %s command received.", key)

		val, err := c.Server.Config.Get(key)
		if err != nil {
			return c.send(nullResponse)
		}

		return c.send(encodeBulkStringArray(2, key, val))

	case "set":
		if len(args) < 3 {
			return fmt.Errorf("insufficient number of arguments for CONFIG SET")
		}

		key := args[1]
		val := args[2]
		fmt.Printf("CONFIG SET %s: %q command received.", key, val)

		if err := c.Server.Config.Add(key, val); err != nil {
			return err
		}

		return c.send(okResponse)
	}

	return nil
}

// handleKeys handles KEY commands.
func (c *ClientHandler) handleKeys(args []string) error {
	var key, result string
	if len(args) > 0 {
		key = args[0]
		val, err := c.Store.Get(key)
		if err != nil {
			return err
		}
		result = encodeBulkStringArray(1, val)
	} else {
		key = "*"
		val, err := c.Store.Get(key)
		if err != nil {
			return err
		}
		result = val // "*" key will return an already-encoded array
	}
	fmt.Printf("KEYS %s command received.", key)

	return c.send(result)
}

// handleInfo handles INFO commands.
func (c *ClientHandler) handleInfo(_ []string) error {
  info := "role:master"
  role, _ := c.Server.Config.Get(replicaOf)
  if role != "" {
    info = "role:slave"
    return c.send(encodeBulkString(info))
  }
  return c.send(encodeBulkString(info + "\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0"))
}

// send sends the message to the client.
func (c *ClientHandler) send(msg string) error {
	_, err := c.Conn.Write([]byte(msg))
	if err != nil {
		return fmt.Errorf("error sending message: %v", err)
	}
	return nil
}

// dbFile returns a pointer to the database file, if there is one configured.
func (c *ClientHandler) dbFile() (*os.File, error) {
	dir, err := c.Server.Config.Get(keyDBDir)
	if err != nil {
		return nil, fmt.Errorf("no database path provided: %v", err)
	}
	filename, err := c.Server.Config.Get(keyDBFilename)
	if err != nil {
		return nil, fmt.Errorf("no database filename provided: %v", err)
	}

	path := filepath.Join(dir, filename)
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("database file not found: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("unable to open file: %v", err)
	}
	return file, nil
}
