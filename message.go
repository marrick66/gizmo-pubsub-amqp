package amqp

import (
	"errors"
	"time"

	"github.com/streadway/amqp"
)

//AMQP specific implementation of the SubscriberMessage interface.
type amqpMessage struct {
	autoAck  bool
	delivery *amqp.Delivery
}

func (m *amqpMessage) Message() []byte {
	return m.delivery.Body
}

func (m *amqpMessage) ExtendDoneDeadline(time.Duration) error {
	//Currently don't see any functionality that is relevant to this.
	return errors.New("Not currently implemented")
}

func (m *amqpMessage) Done() error {
	if m.autoAck {
		return errors.New("AutoAck is set, cannot call Done()")
	}
	//To keep the one-to-one on message to done call relationship, don't
	//clear out any other un-acked messages.
	return m.delivery.Ack(false)
}
