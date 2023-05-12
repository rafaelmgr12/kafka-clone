package entity

import (
	"bufio"
	"io"
	"os"
)

type Consumer struct {
	ID        string
	TopicFile os.File
	Reader    *bufio.Reader
	Conn      io.ReadWriteCloser

	Name     string
	MetaFile os.File
	Meta     *MetaConsumer
	Done     chan struct{}
}

type MetaConsumer struct {
	Offset uint `json:"offset"`
}
