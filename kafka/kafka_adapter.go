package kafkaadp

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/go-yaaf/yaaf-common/config"
	. "github.com/go-yaaf/yaaf-common/messaging"
	"net/url"
	"strings"
	"time"
)

type kafkaAdapter struct {
	client      *kafka.AdminClient
	config      *kafka.ConfigMap
	subscribers map[string]chan bool
}

// NewKafkaMessageBus factory method for Kafka IMessageBus implementation
// param: URI - represents the redis connection string in the format of: kafka://host:port
func NewKafkaMessageBus(URI string) (mq IMessageBus, err error) {

	uri, err := url.Parse(URI)
	if err != nil {
		return nil, fmt.Errorf("parsing URI: %s failed: %s", URI, err.Error())
	}

	conf := &kafka.ConfigMap{}
	conf.SetKey("bootstrap.servers", uri.Host)
	conf.SetKey("go.batch.producer", true)
	conf.SetKey("request.required.acks", "1")
	if strings.ToLower(config.Get().LogLevel()) == "debug" {
		conf.SetKey("debug", "protocol")
	}

	if client, er := kafka.NewAdminClient(conf); err != nil {
		return nil, er
	} else {
		adapter := &kafkaAdapter{
			client:      client,
			config:      conf,
			subscribers: make(map[string]chan bool)}
		return adapter, nil
	}
}

// Ping Test connectivity for retries number of time with time interval (in seconds) between retries
func (r *kafkaAdapter) Ping(retries uint, intervalInSeconds uint) (err error) {

	for i := 0; i < int(retries); i++ {
		if _, err = r.client.ClusterID(context.Background()); err != nil {
			time.Sleep(time.Second * time.Duration(intervalInSeconds))
		} else {
			return nil
		}
	}
	return fmt.Errorf("could not establish connection after %d retries: %s", retries, err)
}

// Close connection and free client resources
func (r *kafkaAdapter) Close() error {
	r.client.Close()
	return nil
}

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// convert raw data to message
//func rawToMessage(factory MessageFactory, bytes []byte) (IMessage, error) {
//	message := factory()
//	if err := json.Unmarshal(bytes, &message); err != nil {
//		return nil, err
//	} else {
//		return message, nil
//	}
//}
//
//// convert message to raw data
//func messageToRaw(message IMessage) ([]byte, error) {
//	return json.Marshal(message)
//}

// endregion
