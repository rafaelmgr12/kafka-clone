package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/rafaelmgr12/kafka-clone/internal/infra"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan struct{}, 1)
	go func() {
		<-sigs
		fmt.Println("shutting down services...")
		done <- struct{}{}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "9001"
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:"+port)
	if err != nil {
		println("ResolveTCPAddr failed:", err.Error())
		os.Exit(1)
	}

	listen, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err)
	}
	defer listen.Close()
	fmt.Printf("listening on port %s...\n", port)

	path := os.Getenv("K_PATH")
	if path == "" {
		path = "data"
	}

	w := os.Getenv("K_WORKERS")
	if w == "" {
		w = "5"
	}

	workers, err := strconv.Atoi(w)
	if err != nil {
		panic(err)
	}

	conf := infra.Config{
		Path:    path,
		Workers: uint(workers),
	}
	infra.Start(conf, listen, done)
}
