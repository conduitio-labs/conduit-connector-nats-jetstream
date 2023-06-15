package internal

import "github.com/nats-io/nats.go"

type NATSClient interface {
	JetStream(...nats.JSOpt) (nats.JetStreamContext, error)
	IsConnected() bool
	Drain() error
	Close()
}
