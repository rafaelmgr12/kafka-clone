package integration_test

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rafaelmgr12/kafka-clone/client"
	"github.com/rafaelmgr12/kafka-clone/internal/domain/entity"
)

type CommunicationStage struct {
	t *testing.T

	messages            map[string]chan entity.Message
	consumerConnections map[string]net.Conn
}

func NewCommunicationStage(t *testing.T) (*CommunicationStage, *CommunicationStage, *CommunicationStage) {
	stage := CommunicationStage{
		t:                   t,
		messages:            make(map[string]chan entity.Message),
		consumerConnections: make(map[string]net.Conn),
	}
	cleanUpFiles("./data")

	return &stage, &stage, &stage
}
func newConnection() (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:9001")
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (s *CommunicationStage) and() *CommunicationStage {
	return s
}

func (s *CommunicationStage) a_consumer_is_running(consumer, topic string) *CommunicationStage {
	conn, err := newConnection()
	if err != nil {
		s.t.Error(err)
		return s
	}
	s.consumerConnections[consumer] = conn
	messages, err := client.Consume(conn, topic, consumer)
	if err != nil {
		s.t.Error(err)
		return s
	}

	s.messages[consumer] = messages

	return s
}

func (s *CommunicationStage) consumer_is_down(consumer string) *CommunicationStage {
	conn, ok := s.consumerConnections[consumer]
	if !ok {
		s.t.Errorf("connection for consumer %s not found", consumer)
		return s
	}

	delete(s.messages, consumer)

	if err := conn.Close(); err != nil {
		s.t.Error(err)
	}
	return s
}

func (s *CommunicationStage) server_is_down() *CommunicationStage {
	serverShutDown <- struct{}{}
	return s
}

func (s *CommunicationStage) server_is_up() *CommunicationStage {
	serverStartUp <- struct{}{}
	time.Sleep(1 * time.Second)
	return s
}

func (s *CommunicationStage) publish_message(message string, topic string) *CommunicationStage {
	conn, err := newConnection()
	if err != nil {
		s.t.Error(err)
		return s
	}
	if err = client.Publish(conn, message, topic); err != nil {
		s.t.Error(err)
		return s
	}

	time.Sleep(200 * time.Millisecond)
	return s
}

func (s *CommunicationStage) publish_concurrent_messages(count int, topic string) *CommunicationStage {
	tasks := make(chan string, 1000)
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < 1000; i++ {
		go func() {
			for {
				select {
				case t, ok := <-tasks:
					if !ok {
						return
					}
					s.publish_message(t, topic)
					wg.Done()
				}
			}
		}()
	}

	for i := 0; i < count; i++ {
		m := fmt.Sprintf("concurrent message %d", i)
		tasks <- m
	}
	close(tasks)

	wg.Wait()
	return s
}

func (s *CommunicationStage) consumer_receives_messages(consumer string, expectedMessages []entity.Message) *CommunicationStage {
	i := 0

	if _, ok := s.messages[consumer]; !ok {
		s.t.Errorf("no consumer %s running", consumer)
		return s
	}

	timer := time.NewTimer(600 * time.Millisecond)
FOR:
	for {
		select {
		case m := <-s.messages[consumer]:
			if len(expectedMessages) <= i {
				s.t.Errorf("expected %d messages, found a %dth message", len(expectedMessages), i)
				break FOR
			}
			if expectedMessages[i].Body != m.Body {
				s.t.Errorf("consumer did not receive expected message: i=%d, expected=%s, found=%s", i, expectedMessages[i].Body, m.Body)
				break FOR
			}
			i++
		case <-timer.C:
			break FOR
		}
	}

	if i != len(expectedMessages) {
		s.t.Errorf("expected %d messages, found %d", len(expectedMessages), i)
	}

	return s
}

func (s *CommunicationStage) consumer_receives_concurrent_messages(count int, consumer string) *CommunicationStage {
	expectedMessages := make(map[string]bool)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("concurrent message %d", i)
		expectedMessages[key] = false
	}

	if _, ok := s.messages[consumer]; !ok {
		s.t.Errorf("no consumer %s running", consumer)
		return s
	}

	i := 0
	timer := time.NewTimer(600 * time.Millisecond)
FOR:
	for {
		select {
		case m := <-s.messages[consumer]:
			if len(expectedMessages) <= i {
				s.t.Errorf("expected %d messages, found a %dth message", len(expectedMessages), i)
				break FOR
			}
			if _, ok := expectedMessages[m.Body]; !ok {
				s.t.Errorf("consumer did receive unexpected message: %s", m.Body)
				break FOR
			}
			expectedMessages[m.Body] = true
			i++
		case <-timer.C:
			break FOR
		}
	}

	if i != count {
		s.t.Errorf("expected %d messages, found %d", count, i)
	}

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("concurrent message %d", i)
		if !expectedMessages[key] {
			s.t.Errorf("consumer did not received expected message: %s", key)
			break
		}
	}

	return s
}
