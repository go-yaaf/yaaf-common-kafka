package kafka

import (
	"fmt"
	"time"

	_ "encoding/json"
	_ "github.com/google/uuid"

	_ "github.com/go-yaaf/yaaf-common/logger"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

// region Message Bus actions ------------------------------------------------------------------------------------------

// Publish messages to a channel (topic)
func (r *kafkaAdapter) Publish(messages ...IMessage) error {

	if len(messages) == 0 {
		return nil
	}

	// Create topics map
	//topicsMap := make(map[string]*pubsub.Topic)
	//for _, message := range messages {
	//	if _, ok := topicsMap[message.Topic()]; !ok {
	//		if topic, err := r.getOrCreateTopic(message.Topic()); err != nil {
	//			return err
	//		} else {
	//			topicsMap[message.Topic()] = topic
	//		}
	//	}
	//}

	// Send messages to topic
	//for _, message := range messages {
	//	if bytes, er := messageToRaw(message); er != nil {
	//		return er
	//	} else {
	//		if topic, ok := topicsMap[message.Topic()]; ok {
	//			_ = topic.Publish(context.Background(), &pubsub.Message{Data: bytes})
	//		}
	//	}
	//}
	return nil
}

// Subscribe on topics
func (r *kafkaAdapter) Subscribe(factory MessageFactory, callback SubscriptionCallback, subscriberName string, topics ...string) (subscriptionId string, error error) {

	if len(topics) != 1 {
		return "", fmt.Errorf("only one topic allawed in this implementation")
	}

	return "", nil
}

// Unsubscribe with the given subscriber id
func (r *kafkaAdapter) Unsubscribe(subscriptionId string) bool {
	return false
}

// Push Append one or multiple messages to a queue
func (r *kafkaAdapter) Push(messages ...IMessage) error {
	return fmt.Errorf("push is not supported in Apache Kafka implementation, use Producer.Publish()")
}

// Pop Remove and get the last message in a queue or block until timeout expires
func (r *kafkaAdapter) Pop(factory MessageFactory, timeout time.Duration, queue ...string) (IMessage, error) {
	return nil, fmt.Errorf("pop is not supported in Apache Kafka implementation, use Consumer.Subscribe()")
}

// CreateProducer creates message producer for specific topic
func (r *kafkaAdapter) CreateProducer(topicName string) (IMessageProducer, error) {
	//if topic, err := r.getOrCreateTopic(topicName); err != nil {
	//	return nil, err
	//} else {
	//	return &kafkaProducer{topicName: topicName, topic: topic}, nil
	//}

	r.client.CreateTopics()
	return nil, nil
}

// endregion

// region Producer actions ---------------------------------------------------------------------------------------------

type kafkaProducer struct {
	topicName string
}

// Close producer does nothing in this implementation
func (p *kafkaProducer) Close() error {
	return nil
}

// Publish messages to a producer channel (topic)
func (p *kafkaProducer) Publish(messages ...IMessage) error {

	if len(messages) == 0 {
		return nil
	}

	return nil
}

// endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// Get topic or create it if not exists
//func (r *kafkaAdapter) getOrCreateTopic(topicName string) (topic *pubsub.Topic, error error) {
//
//}

// Get reference to existing subscription or create new topic if not exists
//func (r *kafkaAdapter) getOrCreateSubscription(topic *pubsub.Topic, subscriberName string) (*pubsub.Subscription, error) {
//
//}

// endregion
