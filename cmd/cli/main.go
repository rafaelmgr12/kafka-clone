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
	host := getEnv("HOST", "localhost")
	port := getEnv("PORT", "9001")

	fmt.Printf("connecting on %s:%s\n", host, port)
	conn, err := net.Dial("tcp", host+":"+port)
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

	handleFlags(*flagConsumer, *flagPublisher, *topic, *consumerName, *message, conn)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func handleFlags(isConsumer, isPublisher bool, topic, consumerName, message string, conn net.Conn) {
	if !isConsumer && !isPublisher {
		println("Must specify either consumer or publisher flag")
		os.Exit(3)
	}

	if isConsumer && isPublisher {
		println("Cannot specify both consumer or publisher flags")
		os.Exit(4)
	}

	if topic == "" {
		println("Must specify the topic")
		os.Exit(5)
	}

	if isConsumer {
		handleConsumer(consumerName, topic, conn)
	} else if isPublisher {
		handlePublisher(message, topic, conn)
	}
}

func handleConsumer(consumerName, topic string, conn net.Conn) {
	if consumerName == "" {
		println("Must specify the consumer name to publish")
		os.Exit(6)
	}

	messages, err := client.Consume(conn, topic, consumerName)
	if err != nil {
		println(err)
		os.Exit(7)
	}

	for m := range messages {
		b, _ := json.Marshal(m)
		println(string(b))
	}
}

func handlePublisher(message, topic string, conn net.Conn) {
	if message == "" {
		println("Must specify the message to publish")
		os.Exit(8)
	}

	if err := client.Publish(conn, message, topic); err != nil {
		println(err)
		os.Exit(9)
	}
}
