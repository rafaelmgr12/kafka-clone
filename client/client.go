package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/google/uuid"
	"github.com/rafaelmgr12/kafka-clone/internal/domain/entity"
)

func Publish(conn net.Conn, body, topic string) error {
	commandBody := entity.Message{
		Headers: map[string]string{
			"id": uuid.New().String(),
		},
		Body: body,
	}

	bodyRaw, err := json.Marshal(commandBody)
	if err != nil {
		return err
	}
	cmd := entity.Command{
		Type:  entity.TypePublish,
		Topic: topic,
		Body:  string(bodyRaw),
	}

	raw, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(conn, string(raw))

	return err

}

func Consume(conn net.Conn, topic, consumerName string) (chan entity.Message, error) {
	cmd := entity.Command{
		Type:  entity.TypeConsume,
		Topic: topic,
		Body:  consumerName,
	}

	messages := make(chan entity.Message)
	raw, err := json.Marshal(cmd)

	if err != nil {
		return messages, err
	}

	if _, err = fmt.Fprintln(conn, string(raw)); err != nil {
		return messages, err
	}

	reader := bufio.NewReader(conn)
	go func() {
		defer close(messages)
		for {
			reply, _, err := reader.ReadLine()
			if err == io.EOF {
				return
			}
			if err != nil {
				fmt.Println(err)
				return
			}
			var response entity.Response
			if err := json.Unmarshal(reply, &response); err != nil {
				continue
			}
			var message entity.Message
			if err := json.Unmarshal([]byte(response.Body), &message); err != nil {
				continue
			}
			messages <- message
		}
	}()
	return messages, nil
}
