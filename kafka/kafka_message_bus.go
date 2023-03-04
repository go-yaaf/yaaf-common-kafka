package kafkaadp

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/go-yaaf/yaaf-common/config"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
	"time"

	_ "encoding/json"
	_ "github.com/google/uuid"

	_ "github.com/go-yaaf/yaaf-common/logger"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

// region Message Bus actions ------------------------------------------------------------------------------------------

// Publish messages to a channel (topic)
func (r *kafkaAdapter) Publish(messages ...IMessage) error {
	return fmt.Errorf("publush is not supported in Apache Kafka implementation, use Producer.Publish()")
}

// Subscribe on topics
func (r *kafkaAdapter) Subscribe(factory MessageFactory, callback SubscriptionCallback, subscriberName string, topics ...string) (subscriptionId string, error error) {

	bootstrapServers, err := r.config.Get("bootstrap.servers", "localhost:9200")
	if err != nil {
		return "", err
	}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"broker.address.family":    "v4",
		"group.id":                 subscriberName,
		"session.timeout.ms":       60000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": true,
	})

	err = c.SubscribeTopics(topics, nil)
	sId := entity.ID()
	sChannel := make(chan bool)
	r.subscribers[sId] = sChannel

	go func() {
		run := true

		for run {
			select {
			case sig := <-sChannel:
				fmt.Printf("Caught termination signal %v: terminating", sig)
				run = false
			default:
				ev := c.Poll(100)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					message := factory()
					if er := json.Unmarshal(e.Value, message); er != nil {
						return
					}
					go callback(message)
				default:
					// fmt.Printf("Ignored %v", e)
				}
			}
		}
		logger.Error("closing consumer")
		_ = c.Close()
	}()
	return sId, nil
}

// Unsubscribe with the given subscriber id
func (r *kafkaAdapter) Unsubscribe(subscriptionId string) bool {
	if ch, ok := r.subscribers[subscriptionId]; !ok {
		return false
	} else {
		ch <- true
		return true
	}
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

	producer, err := kafka.NewProducer(r.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka Producer: %s", err.Error())
	}

	var md *kafka.Metadata

	// Try to connect to kafka for 1 minute, exit if connection failed
	if md, err = producer.GetMetadata(&topicName, true, 60*1000); err != nil {
		return nil, fmt.Errorf("failed to connect to Kafka broker: %s", err.Error())
	}

	// Ensure topic exists and create it if not exists
	if md != nil {
		if _, ok := md.Topics[topicName]; !ok {
			if er := r.createTopics(topicName); er != nil {
				return nil, er
			}
		}
	}

	kp := &kafkaProducer{topicName: topicName, producer: producer, deliveryChan: make(chan kafka.Event)}
	kp.run()
	return kp, nil
}

// endregion

// region Producer actions ---------------------------------------------------------------------------------------------

type kafkaProducer struct {
	topicName    string
	producer     *kafka.Producer
	deliveryChan chan kafka.Event
}

// Close producer does nothing in this implementation
func (p *kafkaProducer) Close() error {
	p.producer.Close()
	return nil
}

// Publish messages to a producer channel (topic)
func (p *kafkaProducer) Publish(messages ...IMessage) error {

	if len(messages) == 0 {
		return nil
	}

	for _, msg := range messages {
		if err := p.publish(msg); err != nil {
			return err
		}
	}
	return nil
}

// Publish single message to a producer channel (topic)
func (p *kafkaProducer) publish(message IMessage) error {

	// Set partition ket by message addressee
	topicPartition := kafka.TopicPartition{Topic: &p.topicName, Partition: kafka.PartitionAny}
	var partitionKey []byte = nil
	if len(message.Addressee()) > 0 {
		partitionKey = []byte(message.Addressee())
	}

	// Marshal message to byte array
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error marshaling message to Json: %s", err.Error())
	}

	err = p.producer.Produce(&kafka.Message{
		TopicPartition: topicPartition,
		Value:          data,
		Key:            partitionKey,
	}, p.deliveryChan)

	if err != nil {
		return fmt.Errorf("error publish message to Kafka topic: %s: %s", p.topicName, err.Error())
	}
	return nil
}

// listen to Kafka delivery channel to track delivery errors
func (p *kafkaProducer) run() {
	pump := func() {
		for {
			select {
			case e := <-p.deliveryChan:
				m := e.(*kafka.Message)

				if m.TopicPartition.Error != nil {
					logger.Error("Error delivering message via KAFKA: %s", m.TopicPartition.Error)
				} else {
					logger.Debug("Message delivered to KAFKA topic %s [%d] at offset %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			}
		}
	}
	go pump()
}

// endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// Get topic or create it if not exists
func (r *kafkaAdapter) createTopics(topics ...string) (err error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	specs := make([]kafka.TopicSpecification, 0)
	for _, topic := range topics {
		spec := kafka.TopicSpecification{
			Topic:         topic,
			NumPartitions: config.Get().TopicPartitions(),
		}
		specs = append(specs, spec)
	}

	_, err = r.client.CreateTopics(ctx, specs, kafka.SetAdminValidateOnly(false))

	if err != nil {
		return fmt.Errorf("error creating KAFKA topic: %s", err.Error())
	} else {
		return nil
	}
}

// endregion
