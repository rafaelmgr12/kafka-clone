package entity

import (
	"net"
)

const (
	TypePublish = iota
	TypeConsume
	TypeClose
)

type Command struct {
	Type         int    `json:"type"`
	Topic        string `json:"topic"`
	Body         string `json:"body"`
	ConsumerName string `json:"consumer_name"`
	Offset       uint   `json:"offset"`
	Connection   net.Conn
}

type Response struct {
	Offset uint   `json:"offset"`
	Body   string `json:"body"`
}
