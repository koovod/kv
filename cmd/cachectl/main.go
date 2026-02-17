package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"github.com/peterh/liner"
)

func main() {
	addr := flag.String("a", "127.0.0.1:6380", "cached tcp address")
	script := flag.String("f", "", "script file to execute")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("connect error: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	if *script != "" {
		if err := runScript(reader, writer, *script); err != nil {
			log.Fatal(err)
		}
		return
	}

	ln := liner.NewLiner()
	defer ln.Close()
	ln.SetCtrlCAborts(true)
	for {
		line, err := ln.Prompt("cache> ")
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		ln.AppendHistory(line)
		args := splitArgs(line)
		if err := sendCommand(writer, args); err != nil {
			fmt.Println("ERR", err)
			continue
		}
		resp, err := readResponse(reader)
		if err != nil {
			fmt.Println("ERR", err)
			continue
		}
		fmt.Println(resp)
	}
}

func runScript(r *bufio.Reader, w *bufio.Writer, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		args := splitArgs(line)
		if err := sendCommand(w, args); err != nil {
			return err
		}
		resp, err := readResponse(r)
		if err != nil {
			return err
		}
		fmt.Println(resp)
	}
	return nil
}

func splitArgs(line string) []string {
	fields := strings.Fields(line)
	out := make([]string, len(fields))
	copy(out, fields)
	return out
}

func sendCommand(w *bufio.Writer, args []string) error {
	if _, err := w.WriteString(fmt.Sprintf("*%d\r\n", len(args))); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := w.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)); err != nil {
			return err
		}
	}
	return w.Flush()
}

func readResponse(r *bufio.Reader) (string, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	switch prefix {
	case '+', '-', ':':
		line, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		return string(prefix) + strings.TrimSpace(line), nil
	case '$':
		var length int
		if _, err := fmt.Fscan(r, &length); err != nil {
			return "", err
		}
		if _, err := r.ReadString('\n'); err != nil {
			return "", err
		}
		if length == -1 {
			return "(nil)", nil
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		if _, err := r.ReadString('\n'); err != nil {
			return "", err
		}
		return string(buf), nil
	default:
		return "", fmt.Errorf("unknown prefix %q", prefix)
	}
}
