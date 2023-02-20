package kafka

import (
	. "github.com/go-yaaf/yaaf-common/messaging"
)

type kafkaAdapter struct {
	//client *k.AdminClient
}

// NewKafkaMessageBus factory method for Kafka IMessageBus implementation
//
// param: URI - represents the redis connection string in the format of: kafka://host:port
func NewKafkaMessageBus(URI string) (mq IMessageBus, err error) {

	//uri, err := url.Parse(URI)
	//
	//if err != nil {
	//	return nil, err
	//}

	//conf := &k.ConfigMap{}
	//conf.SetKey("bootstrap.servers", "")
	//
	//"bootstrap.servers":     a.kafkaBrokerEndpoints,
	//	"go.batch.producer":     true,
	//	"request.required.acks": "1",
	//	"message.max.bytes":     GetBaseConfig().KafkaMaxMessageSizeMb() << (10 * 2),
	//
	//if client, er := k.NewAdminClient(conf); err != nil {
	//	return nil, er
	//} else {
	//	adapter := &kafkaAdapter{client: client}
	//	return adapter, nil
	//}
	return nil, nil
}

// Ping Test connectivity for retries number of time with time interval (in seconds) between retries
func (r *kafkaAdapter) Ping(retries uint, intervalInSeconds uint) error {

	// TODO: implement PING method
	return nil
}

func (r *kafkaAdapter) Close() error {
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
