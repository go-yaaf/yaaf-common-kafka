GO-YAAF Kafka Middleware
=================
![Project status](https://img.shields.io/badge/version-1.2-green.svg)
[![Build](https://github.com/go-yaaf/yaaf-common-kafka/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-kafka/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/go-yaaf/yaaf-common-kafka/badge.svg?branch=main&service=github)](https://coveralls.io/github/go-yaaf/yaaf-common-kafka?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-yaaf/yaaf-common-kafka)](https://goreportcard.com/report/github.com/go-yaaf/yaaf-common-kafka)
[![GoDoc](https://godoc.org/github.com/go-yaaf/yaaf-common-kafka?status.svg)](https://pkg.go.dev/github.com/go-yaaf/yaaf-common-kafka)
![License](https://img.shields.io/dub/l/vibe-d.svg)


This library contains [Apache Kafka](https://kafka.apache.org/) based implementation of the following middleware interfaces:
- The messaging patterns defined by the `IMessageBus` interface of the `yaaf-common` library.

Installation
------------

Use go get.

	go get -v -t github.com/go-yaaf/yaaf-common-kafka

Then import the validator package into your own code.

	import "github.com/go-yaaf/yaaf-common-kafka"



