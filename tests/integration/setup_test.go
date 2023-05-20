package integration_test

import (
	"net"
	"os"
	"testing"

	"github.com/rafaelmgr12/kafka-clone/internal/infra"
)

var serverShutDown chan struct{}
var serverStartUp chan struct{}

func TestMain(m *testing.M) {
	serverShutDown = make(chan struct{}, 1)
	serverStartUp = make(chan struct{})

	go func() {
		for {
			println("wait server start up channel")
			select {
			// support to start server multiple times
			case <-serverStartUp:
				println("starting server")
				tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:9001")
				if err != nil {
					println("ResolveTCPAddr failed:", err.Error())
					os.Exit(1)
				}

				listen, err := net.ListenTCP("tcp", tcpAddr)
				if err != nil {
					panic(err)
				}
				conf := infra.Config{
					Path:    "./data",
					Workers: 5,
				}
				infra.Start(conf, listen, serverShutDown)
				if err = listen.Close(); err != nil {
					panic(err)
				}
				println("server stopped")
			}
		}
	}()

	serverStartUp <- struct{}{}

	defer func() {
		serverShutDown <- struct{}{}
	}()

	m.Run()
}

func cleanUpFiles(root string) error {
	os.RemoveAll(root)
	return os.MkdirAll(root, 0775)
}
