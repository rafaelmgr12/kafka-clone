package infra

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/rafaelmgr12/kafka-clone/internal/domain/entity"
	"github.com/rafaelmgr12/kafka-clone/internal/domain/usecases"
)

var consumers map[string]entity.Consumer

type Config struct {
	Path    string
	Workers uint
}

func Start(conf Config, listen *net.TCPListener, done <-chan struct{}) {
	consumers = make(map[string]entity.Consumer)
	commands := make(chan entity.Command)
	stopCommands := make(chan bool, 1)

	go waitForCommands(listen, commands, stopCommands)
	for i := 0; i < int(conf.Workers); i++ {
		go handleCommands(conf.Path, commands)
	}
	<-done
	close(stopCommands)
	log.Println("closing consumers...")
	for _, consumer := range consumers {
		consumer.Close()
	}
}

func handleCommands(path string, commands chan entity.Command) {
	for c := range commands {
		if err := routeCommand(c, path); err != nil {
			log.Printf("error on routing command: %s", err)
		}
	}
}

func waitForCommands(listen *net.TCPListener, commands chan entity.Command, stopCommands chan bool) {
	defer close(commands)
	for {
		listen.SetDeadline(time.Now().Add(200 * time.Millisecond))
		conn, err := listen.AcceptTCP()
		if err != nil {
			if softError(err) {
				continue
			}
			log.Printf("unable to accept tcp connection: %s\n", err)
			continue
		}
		if err := conn.SetKeepAlive(true); err != nil {
			log.Printf("unable to set keep alive: %s\n", err)
		}
		go handleConnection(conn, commands, stopCommands)
	}
}

func handleConnection(conn net.Conn, commands chan entity.Command, stopCommands chan bool) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			command := entity.Command{Type: entity.TypeClose, Connection: conn}
			commands <- command
			return
		}
		if err != nil {
			if closedConnection(err) {
				return
			}
			log.Printf("unable to read connection: %s\n", err)
			continue
		}
		var command entity.Command
		if err = json.Unmarshal(line, &command); err != nil {
			log.Printf("error on json: %s\n", err)
			continue
		}
		command.Connection = conn
		select {
		case <-stopCommands:
			return
		case commands <- command:
		}
	}
}

func routeCommand(c entity.Command, path string) error {
	commandNames := map[int]string{
		entity.TypeClose:   "close",
		entity.TypeConsume: "consume",
		entity.TypePublish: "publish",
	}
	log.Printf("received command type=%s \n", commandNames[c.Type])

	switch c.Type {
	case entity.TypePublish:
		var message entity.Message
		if err := json.Unmarshal([]byte(c.Body), &message); err != nil {
			return err
		}
		return usecases.Publish(c.Connection, message, c.Topic, path)
	case entity.TypeConsume:
		consumer, err := entity.NewConsumer(c.ConsumerName, c.Connection, c.Topic, path)
		if err != nil {
			return err
		}
		consumers[consumer.FileName()] = consumer
		go consumer.Start()
		return nil
	case entity.TypeClose:
		closeConsumer(c.Connection)
		return nil
	}

	return fmt.Errorf("no expected command type: %d\n", c.Type)
}

func closeConsumer(conn net.Conn) {
	for key, consumer := range consumers {
		if consumer.Conn == conn {
			consumer.Close()
			delete(consumers, key)
		}
	}
}

func softError(err error) bool {
	if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
		return true
	}
	return closedConnection(err)
}

func closedConnection(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}
