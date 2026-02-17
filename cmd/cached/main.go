package main

import (
	"bufio"
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/komalk/cashe/internal/protocol"
	"github.com/komalk/cashe/internal/store"
	"github.com/komalk/cashe/pkg/api"
)

func main() {
	var (
		httpAddr = flag.String("http-addr", ":8080", "HTTP listen address")
		tcpAddr  = flag.String("tcp-addr", ":6380", "RESP listen address")
		shards   = flag.Int("shards", 64, "number of store shards")
		cleanInt = flag.Duration("clean-interval", 250*time.Millisecond, "ttl sweep interval")
	)
	flag.Parse()

	st := store.NewStore(store.StoreOptions{Shards: *shards, CleanInterval: *cleanInt})
	defer st.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go runHTTP(*httpAddr, st)
	go runTCP(*tcpAddr, st)

	<-ctx.Done()
	log.Println("shutting down")
}

func runHTTP(addr string, st *store.Store) {
	srv := &http.Server{
		Addr:    addr,
		Handler: api.NewServer(st).Router(),
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("http server error: %v", err)
	}
}

func runTCP(addr string, st *store.Store) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("tcp listen error: %v", err)
	}
	log.Printf("tcp server listening on %s", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConn(conn, st)
	}
}

func handleConn(conn net.Conn, st *store.Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		cmd, err := protocol.ReadArray(reader)
		if err != nil {
			protocol.WriteError(writer, "ERR "+err.Error())
			return
		}
		if len(cmd) == 0 {
			continue
		}
		switch strings.ToUpper(cmd[0]) {
		case "PING":
			protocol.WriteSimple(writer, "PONG")
		case "SET":
			handleSet(writer, st, cmd)
		case "GET":
			handleGet(writer, st, cmd)
		case "DEL":
			handleDel(writer, st, cmd)
		case "INCR":
			handleIncr(writer, st, cmd)
		case "QUIT":
			protocol.WriteSimple(writer, "OK")
			return
		default:
			protocol.WriteError(writer, "ERR unknown command")
		}
	}
}

func handleSet(w *bufio.Writer, st *store.Store, cmd []string) {
	if len(cmd) < 3 {
		protocol.WriteError(w, "ERR wrong number of arguments")
		return
	}
	key, value := cmd[1], []byte(cmd[2])
	var ttl time.Duration
	if len(cmd) > 3 {
		if strings.ToUpper(cmd[3]) == "EX" && len(cmd) > 4 {
			sec, err := time.ParseDuration(cmd[4] + "s")
			if err != nil {
				protocol.WriteError(w, "ERR invalid expire")
				return
			}
			ttl = sec
		}
	}
	st.Set(key, value, ttl)
	protocol.WriteSimple(w, "OK")
}

func handleGet(w *bufio.Writer, st *store.Store, cmd []string) {
	if len(cmd) != 2 {
		protocol.WriteError(w, "ERR wrong number of arguments")
		return
	}
	if val, ok := st.Get(cmd[1]); ok {
		protocol.WriteBulk(w, val)
		return
	}
	protocol.WriteBulk(w, nil)
}

func handleDel(w *bufio.Writer, st *store.Store, cmd []string) {
	if len(cmd) < 2 {
		protocol.WriteError(w, "ERR wrong number of arguments")
		return
	}
	removed := st.Del(cmd[1:]...)
	protocol.WriteInteger(w, int64(removed))
}

func handleIncr(w *bufio.Writer, st *store.Store, cmd []string) {
	if len(cmd) < 2 {
		protocol.WriteError(w, "ERR wrong number of arguments")
		return
	}
	var delta int64 = 1
	if len(cmd) > 2 {
		d, err := strconv.ParseInt(cmd[2], 10, 64)
		if err != nil {
			protocol.WriteError(w, "ERR delta must be integer")
			return
		}
		delta = d
	}
	v, err := st.Incr(cmd[1], delta)
	if err != nil {
		protocol.WriteError(w, "ERR "+err.Error())
		return
	}
	protocol.WriteInteger(w, v)
}
