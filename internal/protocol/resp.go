package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

var (
	errInvalidRESP = errors.New("invalid resp payload")
)

// ReadArray reads a RESP array of bulk strings.
func ReadArray(r *bufio.Reader) ([]string, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		return nil, fmt.Errorf("expected array prefix, got %q", prefix)
	}
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(string(line))
	if err != nil || n < 0 {
		return nil, errInvalidRESP
	}
	items := make([]string, 0, n)
	for i := 0; i < n; i++ {
		bulkPrefix, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if bulkPrefix != '$' {
			return nil, errInvalidRESP
		}
		bulkLenLine, err := readLine(r)
		if err != nil {
			return nil, err
		}
		ln, err := strconv.Atoi(string(bulkLenLine))
		if err != nil || ln < 0 {
			return nil, errInvalidRESP
		}
		buf := make([]byte, ln)
		if _, err := r.Read(buf); err != nil {
			return nil, err
		}
		if err := consumeCRLF(r); err != nil {
			return nil, err
		}
		items = append(items, string(buf))
	}
	return items, nil
}

// WriteSimple writes a RESP simple string.
func WriteSimple(w *bufio.Writer, s string) error {
	if _, err := w.WriteString("+" + s + "\r\n"); err != nil {
		return err
	}
	return w.Flush()
}

// WriteError writes RESP error.
func WriteError(w *bufio.Writer, msg string) error {
	if _, err := w.WriteString("-" + msg + "\r\n"); err != nil {
		return err
	}
	return w.Flush()
}

// WriteInteger writes RESP integer.
func WriteInteger(w *bufio.Writer, v int64) error {
	if _, err := w.WriteString(fmt.Sprintf(":%d\r\n", v)); err != nil {
		return err
	}
	return w.Flush()
}

// WriteBulk writes RESP bulk string.
func WriteBulk(w *bufio.Writer, b []byte) error {
	if b == nil {
		if _, err := w.WriteString("$-1\r\n"); err != nil {
			return err
		}
		return w.Flush()
	}
	if _, err := w.WriteString(fmt.Sprintf("$%d\r\n", len(b))); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	if _, err := w.WriteString("\r\n"); err != nil {
		return err
	}
	return w.Flush()
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errInvalidRESP
	}
	return bytes.TrimSuffix(line, []byte("\r\n")), nil
}

func consumeCRLF(r *bufio.Reader) error {
	cr, err := r.ReadByte()
	if err != nil {
		return err
	}
	lf, err := r.ReadByte()
	if err != nil {
		return err
	}
	if cr != '\r' || lf != '\n' {
		return errInvalidRESP
	}
	return nil
}
