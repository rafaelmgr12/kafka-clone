package entity

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

type Consumer struct {
	ID        string
	TopicFile *os.File
	Reader    *bufio.Reader
	Topic     string
	Conn      io.ReadWriteCloser

	Name     string
	MetaFile *os.File
	Meta     *MetaConsumer
	Done     chan struct{}
}

type MetaConsumer struct {
	Offset uint `json:"offset"`
}

func NewConsumer(name string, conn net.Conn, topic, path string) (Consumer, error) {
	id := fmt.Sprintf("/%s/%s", path, fileName(name, topic))
	file, err := os.OpenFile(id, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return Consumer{}, fmt.Errorf("cannot open consumer file: %w", err)
	}

	data, err := io.ReadAll(file)
	if err != nil {
		return Consumer{}, fmt.Errorf("cannot open topic file: %w", err)
	}

	if len(data) == 0 {
		data = []byte("{}")
	}

	var meta MetaConsumer
	if err = json.Unmarshal(data, &meta); err != nil {
		return Consumer{}, fmt.Errorf("cannot open topic file: %w", err)
	}

	filePath := fmt.Sprint("%s/%s.topic", path, topic)
	topicFile, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return Consumer{}, fmt.Errorf("cannot open topic file: %w", err)
	}

	reader := bufio.NewReader(topicFile)
	fmt.Printf("Consumer %s created with offset %d\n", id, meta.Offset)
	for i := uint(0); i < meta.Offset; i++ {
		reader.ReadLine()
	}

	done := make(chan struct{}, 1)
	return Consumer{
		ID:        id,
		Reader:    reader,
		TopicFile: topicFile,
		Name:      name,
		MetaFile:  file,
		Meta:      &meta,
		Done:      done,
		Conn:      conn,
	}, err

}

func (c Consumer) Start() {
	fmt.Printf("%s consuming...\n", c.ID)
	for {
		select {
		case <-c.Done:
			return
		default:
			line, _, err := c.Reader.ReadLine()
			if err == io.EOF {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if err != nil {
				fmt.Printf("%s unable to read topic file: %s", c.ID, err)
				continue
			}

			response := Response{
				Offset: c.Meta.Offset,
				Body:   string(line),
			}

			resp, err := json.Marshal(response)
			if err != nil {
				fmt.Printf("%s unable to marshal response json: %s", c.ID, err)
				continue
			}

			fmt.Fprintln(c.Conn, string(resp))
			c.Meta.Offset++
			c.updateMetaFile()
		}
	}
}
func (c Consumer) updateMetaFile() error {
	data, err := json.Marshal(c.Meta)
	if err != nil {
		return err
	}
	if err = c.MetaFile.Truncate(0); err != nil {
		return err
	}
	c.MetaFile.Seek(0, 0)
	_, err = c.MetaFile.Write(data)

	return err
}

func (c Consumer) Close() {
	fmt.Printf("%s closing...\n", c.ID)
	close(c.Done)
	c.updateMetaFile()
	c.TopicFile.Close()
	c.MetaFile.Close()
	c.Conn.Close()
}

func (c Consumer) FileName() string {
	return fileName(c.Name, c.Topic)
}

func fileName(name, topic string) string {
	return name + "." + topic + ".consumer"
}
