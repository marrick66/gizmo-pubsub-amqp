package amqp

import (
	"github.com/NYTimes/gizmo/pubsub"
	"github.com/streadway/amqp"
)

//Internal implementation of subscribing to an AMQP exchange/topic.
type subscriber struct {
	connStr  string
	exchange string
	topic    string
	options  *QueueOptions
	queue    amqp.Queue
	conn     *amqp.Connection
	ch       *amqp.Channel
	delCh    <-chan amqp.Delivery
	subCh    chan pubsub.SubscriberMessage
	lastErr  error
}

//QueueOptions are optional settings to control fine-grained
//behavior of the associated queue.
type QueueOptions struct {
	Name       string
	AutoAck    bool
	Durable    bool
	AutoDelete bool
	Exclusive  bool
}

//NewSubscriber returns an AMQP implementation of the Subscriber interface.
//If it's unable to connect, the exchange doesn't exist, or fails to setup a
//queue, it returns an error. If an options object is passed,
//it must have all boolean values set.
func NewSubscriber(
	connStr string,
	exchange string,
	topic string,
	options *QueueOptions) (pubsub.Subscriber, error) {

	sub := &subscriber{
		connStr:  connStr,
		exchange: exchange,
		topic:    topic,
		lastErr:  nil}

	if options != nil {
		sub.options = options
	} else {
		sub.options = &QueueOptions{
			Name:       "",
			AutoAck:    true,
			Durable:    false,
			AutoDelete: true,
			Exclusive:  true}
	}

	if err := sub.initQueue(); err != nil {
		return nil, err
	}

	return sub, nil
}

//initQueue is an internal method that attempts to:
//1.  Connect to the broker
//2.  Declare the queue from the subscription QueueOptions
//3.  Bind the queue to an existing exchange. Currently, a previously
//    defined, durable exchange must exist
func (s *subscriber) initQueue() error {
	var err error

	//Only reconnect and setup the channel if needed:
	if s.conn == nil || s.ch == nil || s.conn.IsClosed() {
		if s.conn, err = amqp.Dial(s.connStr); err != nil {
			return err
		}
	}

	if s.ch, err = s.conn.Channel(); err != nil {
		return err
	}

	if s.queue, err = s.ch.QueueDeclare(
		s.options.Name,
		s.options.Durable,
		s.options.AutoDelete,
		s.options.Exclusive,
		false, //wait to make sure it's actually declared.
		nil); err != nil {
		s.conn.Close()
		return err
	}

	if err = s.ch.QueueBind(
		s.options.Name,
		s.topic,
		s.exchange,
		false, //wait to make sure it's actually bound.
		nil); err != nil {
		s.conn.Close()
		return err
	}

	return nil
}

/*
All of the gizmo/pubsub Subscriber interface method implementations:
*/

//Err returns the last known error for the subscriber.
func (s *subscriber) Err() error {
	return s.lastErr
}

//Stop should close the AMQP underlying channel, which should close any
//open delivery channels causing message forwarding to stop.
func (s *subscriber) Stop() error {
	if s.ch != nil {
		return s.ch.Close()
	}
	return nil
}

//Attempt to start consuming messages from the exchange/topic. If the channel
//returned is closed, check the subscription.Err() function to see why.
func (s *subscriber) Start() <-chan pubsub.SubscriberMessage {
	s.subCh = make(chan pubsub.SubscriberMessage)

	var err error

	//Tear down the existing channel prior to initializing a new queue:
	if _, err = s.ch.QueueInspect(s.queue.Name); err == nil {
		s.ch.Close()
	}

	if err = s.initQueue(); err != nil {
		return s.closedChannel(err)
	}

	if s.delCh, err = s.ch.Consume(
		s.options.Name,
		"", //Don't setup a consumer name.
		s.options.AutoAck,
		s.options.Exclusive,
		false, //NoLocal not supported currently.
		false, //Wait for the subscription to complete.
		nil); err != nil {
		return s.closedChannel(err)
	}

	//Start forwarding loop asynchronously.
	go s.forwardMessages(s.delCh, s.subCh)

	return s.subCh
}

/*
Helper methods needed for starting the subscription:
*/

//Converts the raw delivery messages into the AMQP SubscriberMessage
//implementation. When the source channel is closed, we have to close
//destination as well to stop its consumers.
func (s *subscriber) forwardMessages(
	fromCh <-chan amqp.Delivery,
	toCh chan pubsub.SubscriberMessage) {
	for msg := range fromCh {
		toCh <- &amqpMessage{
			autoAck:  s.options.AutoAck,
			delivery: &msg}
	}
	close(toCh)
}

//Sets the last error and closes the subscription channel if it exists.
func (s *subscriber) setErrorStatus(err error) {
	s.lastErr = err
	if s.ch != nil {
		s.ch.Close()
	}
	if s.subCh != nil {
		close(s.subCh)
	}
}

//Helper method to return a closed channel in a failed start.
func (s *subscriber) closedChannel(err error) chan pubsub.SubscriberMessage {
	s.setErrorStatus(err)
	return s.subCh
}
