package usecases

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/rafaelmgr12/kafka-clone/internal/domain/entity"
)

func Publish(conn net.Conn, message entity.Message, topic, path string) error {
	filePath := fmt.Sprintf("%s/%s", path, topic)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	raw, err := json.Marshal(message)
	if err != nil {
		return err
	}

	raw = append(raw, byte('\n'))
	_, err = file.Write(raw)

	return err
}
