# Kafka PubSub example

This example demonstrates publish/subscribe messaging pattern using Kafka implementation of IMessageBus in `yaaf-common` package.
The example initializes one publisher publishing messages to a topic and two subscribers processing these messages

### Run Example
You need a connection to Kafka 3.x server, this can be done by running Kafka in Docker or local Kubernetes

To run this example:

```shell
go run .
```