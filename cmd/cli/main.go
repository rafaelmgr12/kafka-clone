package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/rafaelmgr12/kafka-clone/client"
)

func main() {
	host := os.Getenv("HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "9001"
	}

	fmt.Printf("connecting on %s:%s\n", host, port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", host+":"+port)
	if err != nil {
		println("ResolveTCPAddr failed:", err.Error())
		os.Exit(1)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		println("Dial failed:", err.Error())
		os.Exit(2)
	}

	flagConsumer := flag.Bool("c", false, "use of consumer")
	flagPublisher := flag.Bool("p", false, "use of publisher")
	topic := flag.String("t", "", "topic to consume")
	consumerName := flag.String("n", "", "consumer name")

	message := flag.String("m", "", "message to publish")
	flag.Parse()

	if !*flagConsumer && !*flagPublisher {
		println("Must specify either consumer or publisher flag")
		os.Exit(3)
	}

	if *flagConsumer && *flagPublisher {
		println("Cannot specify both consumer or publisher flags")
		os.Exit(4)
	}

	if *topic == "" {
		println("Must specify the topic")
		os.Exit(5)
	}

	switch true {
	case *flagConsumer:
		if consumerName == nil || *consumerName == "" {
			println("Must specify the consumer name to publish")
			os.Exit(6)
		}

		messages, err := client.Consume(conn, *topic, *consumerName)
		if err != nil {
			println(err)
			os.Exit(7)
		}

		for m := range messages {
			b, _ := json.Marshal(m)
			println(string(b))
		}
	case *flagPublisher:
		if message == nil || *message == "" {
			println("Must specify the message to publish")
			os.Exit(8)
		}

		if err := client.Publish(conn, *message, *topic); err != nil {
			println(err)
			os.Exit(9)
		}
	}

}
