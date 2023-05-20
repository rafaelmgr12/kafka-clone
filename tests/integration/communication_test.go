package integration_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/rafaelmgr12/kafka-clone/internal/domain/entity"
)

func TestSendOneMessage(t *testing.T) {
	given, when, then := NewCommunicationStage(t)

	consumer := "notification"
	topic := "customers"
	given.a_consumer_is_running(consumer, topic)

	id := uuid.NewString()
	when.publish_message("messagem com id"+id, topic)

	messages := []entity.Message{
		{
			Body: "messagem com id" + id,
		},
	}
	then.consumer_receives_messages(consumer, messages)
}

func TestSendMultipleMessages(t *testing.T) {
	given, when, then := NewCommunicationStage(t)

	consumer := "notification"
	topic := "customers"
	given.a_consumer_is_running(consumer, topic)

	id := uuid.NewString()
	when.publish_message("messagem com id"+id, topic)
	when.publish_message("messagem 2 com id"+id, topic)
	when.publish_message("messagem 3 com id"+id, topic)

	messages := []entity.Message{
		{
			Body: "messagem com id" + id,
		},
		{
			Body: "messagem 2 com id" + id,
		},
		{
			Body: "messagem 3 com id" + id,
		},
	}
	then.consumer_receives_messages(consumer, messages)
}

func TestMultipleConsumersInDifferentTimes(t *testing.T) {
	given, when, then := NewCommunicationStage(t)

	consumer1 := "email"
	consumer2 := "sms"
	topic := "customers"
	given.a_consumer_is_running(consumer1, topic)

	id := uuid.NewString()
	when.publish_message("messagem com id"+id, topic)
	when.publish_message("messagem 2 com id"+id, topic)
	when.publish_message("messagem 3 com id"+id, topic)

	messages := []entity.Message{
		{
			Body: "messagem com id" + id,
		},
		{
			Body: "messagem 2 com id" + id,
		},
		{
			Body: "messagem 3 com id" + id,
		},
	}

	then.consumer_receives_messages(consumer1, messages)

	id2 := uuid.NewString()
	when.publish_message("messagem 4 com id"+id2, topic)

	messages = []entity.Message{
		{
			Body: "messagem 4 com id" + id2,
		},
	}
	then.consumer_receives_messages(consumer1, messages)

	when.a_consumer_is_running(consumer2, topic)

	messages = []entity.Message{
		{
			Body: "messagem com id" + id,
		},
		{
			Body: "messagem 2 com id" + id,
		},
		{
			Body: "messagem 3 com id" + id,
		},
		{
			Body: "messagem 4 com id" + id2,
		},
	}
	then.consumer_receives_messages(consumer2, messages)
}

func TestMultipleConsumersAtTheSameTime(t *testing.T) {
	given, when, then := NewCommunicationStage(t)

	consumer1 := "email"
	consumer2 := "sms"
	topic := "customers"
	given.a_consumer_is_running(consumer1, topic).and().
		a_consumer_is_running(consumer2, topic)

	id := uuid.NewString()
	when.publish_message("messagem com id"+id, topic)
	when.publish_message("messagem 2 com id"+id, topic)
	when.publish_message("messagem 3 com id"+id, topic)

	messages := []entity.Message{
		{
			Body: "messagem com id" + id,
		},
		{
			Body: "messagem 2 com id" + id,
		},
		{
			Body: "messagem 3 com id" + id,
		},
	}

	then.consumer_receives_messages(consumer1, messages).and().
		consumer_receives_messages(consumer2, messages)
}

func TestConsumerKeepsWaitingForNewMessages(t *testing.T) {
	given, when, then := NewCommunicationStage(t)

	consumer := "notification"
	topic := "customers"
	given.a_consumer_is_running(consumer, topic)

	id := uuid.NewString()
	when.publish_message("messagem com id"+id, topic)
	when.publish_message("messagem2 com id"+id, topic)

	messages := []entity.Message{
		{
			Body: "messagem com id" + id,
		},
		{
			Body: "messagem2 com id" + id,
		},
	}
	then.consumer_receives_messages(consumer, messages)

	id = uuid.NewString()
	when.publish_message("messagem3 com id"+id, topic).and().
		a_consumer_is_running(consumer, topic)

	messages = []entity.Message{
		{
			Body: "messagem3 com id" + id,
		},
	}
	then.consumer_receives_messages(consumer, messages)
}
func TestSendMessagesWhenConsumerIsDown(t *testing.T) {
	given, when, then := NewCommunicationStage(t)

	consumer := "notification"
	topic := "customers"
	given.a_consumer_is_running(consumer, topic)

	id := uuid.NewString()
	when.publish_message("messagem com id"+id, topic)
	when.publish_message("messagem2 com id"+id, topic)

	messages := []entity.Message{
		{
			Body: "messagem com id" + id,
		},
		{
			Body: "messagem2 com id" + id,
		},
	}
	then.consumer_receives_messages(consumer, messages)

	given.consumer_is_down(consumer)

	id = uuid.NewString()
	when.publish_message("messagem3 com id"+id, topic).and().
		a_consumer_is_running(consumer, topic)

	messages = []entity.Message{
		{
			Body: "messagem3 com id" + id,
		},
	}
	then.consumer_receives_messages(consumer, messages)
}

func TestConsumerRestartsFromOffsetWhenServerWentDown(t *testing.T) {
	given, when, then := NewCommunicationStage(t)

	consumer := "notification"
	topic := "customers"
	given.a_consumer_is_running(consumer, topic)

	id := uuid.NewString()
	when.publish_message("messagem com id"+id, topic)
	when.publish_message("messagem2 com id"+id, topic)

	messages := []entity.Message{
		{
			Body: "messagem com id" + id,
		},
		{
			Body: "messagem2 com id" + id,
		},
	}
	then.consumer_receives_messages(consumer, messages)

	given.consumer_is_down(consumer)

	id = uuid.NewString()
	when.publish_message("messagem3 com id"+id, topic).and().
		server_is_down().and().
		server_is_up()

	given.a_consumer_is_running(consumer, topic)

	messages = []entity.Message{
		{
			Body: "messagem3 com id" + id,
		},
	}
	then.consumer_receives_messages(consumer, messages)
}
