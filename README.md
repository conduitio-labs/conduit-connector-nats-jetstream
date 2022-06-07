# Conduit Connector NATS

### General

The [NATS](https://nats.io/) connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a source and a destination NATS connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2
- [NATS](https://nats.io/download/) 2.8.4 and [nats.go](https://github.com/nats-io/nats.go) library v1.16.0

### How to build it

Run `make`.

### Testing

Run `make test` to run all the unit and integration tests, which require Docker and Docker Compose to be installed and running. The command will handle starting and stopping docker containers for you.

## NATS Source

### Connection and authentication

The NATS connector connects to a NATS server or a cluster with the required parameters `urls`, `subject` and `mode`. If your NATS server has a configured authentication you can pass an authentication details in the connection URL. For example, for a token authentication the url will look like: `nats://mytoken@127.0.0.1:4222`, and for a username/password authentication: `nats://username:password@127.0.0.1:4222`. But if your server is using [NKey](https://docs.nats.io/using-nats/developer/connecting/nkey) or [Credentials file](https://docs.nats.io/using-nats/developer/connecting/creds) for authentication you must configure them via seperate [configuration](#configuration) parameters.

### PubSub

The connector listening on a subject receives messages published on that subject. If the connector is not actively listening on the subject, the message is not received. The connector can use the [wildcard](https://docs.nats.io/nats-concepts/subjects#wildcards) tokens such as `*` and `>` to match a single token or to match the tail of a subject.

### JetStream

The connector creates a durable NATS consumer which means it's able to read messages that were written to a NATS stream before the connector was created, unless configured otherwise. The `deliverPolicy` configuration parameter allows you to control this behavior.

- If the `deliverPolicy` is equal to `new` the connector will only consume messages which were created after the connector.
- If the `deliverPolicy` is equal to `all` the connector will consume all messages in a stream.

The connector allows you to configure a size of a pending message buffer. If your NATS server has hundreds of thousands of messages and a high frequency of their writing, it's highly recommended to set the `bufferSize` parameter high enough (`65536` or more, depending on how much RAM you have). Otherwise, you risk getting a [slow consumers](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers) problem.

### Position handling

The connector goes through two modes.

_PubSub mode_: The position during this mode is a random binary marshaled UUIDv4.

_JetStream mode_: The position during this mode contains the following fields: `durable` (a durable consumer name), `stream` (a name of a stream the consumer reading from), `subject`, `timestamp` (timestamp of a message or the time the message was read by the connector) and `opt_seq` (the position of a message in a stream).

### Configuration

The config passed to Configure can contain the following fields.

| name                      | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | required | default                 | examples                                                                                                                                                               |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `urls`                    | A list of connection URLs joined by comma. Must be a valid URLs                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | **true**     |                         | `nats://127.0.0.1:1222`<br />`nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224`<br />`nats://myname:password@127.0.0.1:4222`<br />`nats://mytoken@127.0.0.1:4222` |
| `subject`                 | A name of a subject from which or to which the connector should read write.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | **true**     |                         |                                                                                                                                                                        |
| `mode`                    | A communication mode to be used, must be either pubsub or jetstream                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | **true**     |
| `connectionName`          | Optional connection name which will come in handy when it comes to monitoring                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | false    |                         |
| `nkeyPath`                | A path pointed to a [NKey](https://docs.nats.io/using-nats/developer/connecting/nkey) pair. Must be a valid file path                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | false    |                         |
| `credentialsFilePath`     | A path pointed to a [credentials file](https://docs.nats.io/using-nats/developer/connecting/creds). Must be a valid file path                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | false    |                         |
| `tlsClientCertPath`       | A path pointed to a TLS client certificate, must be present if tlsClientPrivateKeyPath field is also present. Must be a valid file path                                                                                                                                                                                                                                                                                                                                                                                                                                                              | false    |                         |
| `tlsClientPrivateKeyPath` | A path pointed to a TLS client private key, must be present if tlsClientCertPath field is also present. Must be a valid file path                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | false    |                         |
| `tlsRootCACertPath`       | A path pointed to a TLS root certificate, provide if you want to verify server’s identity. Must be a valid file path                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | false    |                         |
| `bufferSize`              | A buffer size for consumed messages. It must be set to avoid the [slow consumers](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers) problem. Minimum allowed value is `64`                                                                                                                                                                                                                                                                                                                                                                                                      | false    | `1024`                  |
| `streamName`              | A stream name, only required if mode is equal to `jetstream`. It can contains alphanumeric characters and max length is 32 characters                                                                                                                                                                                                                                                                                                                                                                                                                                                                | false    |                         |
| `durable`                 | The name of the Consumer, if set will make a consumer durable, allowing resuming consumption where left off                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | false    | `conduit_push_consumer` |
| `deliverPolicy`           | Defines where in the stream the connector should start receiving messages. Allowed values are `new` and `all`.<br /><br />-`all` - The connector will start receiving from the earliest available message.<br />-`new` - When first consuming messages, the connector will only start receiving messages that were created after the consumer was created.<br /><br />If the connector starts with non-zero position, the deliver policy will be [DeliverByStartSequence](https://docs.nats.io/nats-concepts/jetstream/consumers#deliverbystartsequence) and the connector will read messages from that position | false    | `all`                   |
| `ackPolicy`               | Defines how messages should be acknowledged.<br />Allowed values are `explicit`, `all` and `none`<br /><br />- `explicit` - each individual message must be acknowledged<br />- `all` - if the connector receives a series of messages, it only has to ack the last one it received<br />- `none` - the connector doesn’t have to ack any messages                                                                                                                                                                                                                                                         | false    | `explicit`              |
