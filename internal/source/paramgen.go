// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package source

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigAckPolicy               = "ackPolicy"
	ConfigBufferSize              = "bufferSize"
	ConfigConnectionName          = "connectionName"
	ConfigCredentialsFilePath     = "credentialsFilePath"
	ConfigDeliverPolicy           = "deliverPolicy"
	ConfigDeliverSubject          = "deliverSubject"
	ConfigDurable                 = "durable"
	ConfigMaxReconnects           = "maxReconnects"
	ConfigNkeyPath                = "nkeyPath"
	ConfigReconnectWait           = "reconnectWait"
	ConfigStream                  = "stream"
	ConfigSubject                 = "subject"
	ConfigTlsClientCertPath       = "tls.clientCertPath"
	ConfigTlsClientPrivateKeyPath = "tls.clientPrivateKeyPath"
	ConfigTlsRootCACertPath       = "tls.rootCACertPath"
	ConfigUrls                    = "urls"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigAckPolicy: {
			Default:     "explicit",
			Description: "AckPolicy defines how messages should be acknowledged.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationInclusion{List: []string{"explicit", "none", "all"}},
			},
		},
		ConfigBufferSize: {
			Default:     "1024",
			Description: "BufferSize is a buffer size for consumed messages.\nIt must be set to avoid the problem with slow consumers.\nSee details about slow consumers here https://docs.nats.io/using-nats/developer/connecting/events/slow.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{
				config.ValidationGreaterThan{V: 64},
			},
		},
		ConfigConnectionName: {
			Default:     "",
			Description: "ConnectionName is the name of the connection that the connector establishes.\nSetting the connection is useful when monitoring the connector.\nThe default value is the connector ID.\nSee https://docs.nats.io/using-nats/developer/connecting/name.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigCredentialsFilePath: {
			Default:     "",
			Description: "CredentialsFilePath is the path to a credentials file.\nSee https://docs.nats.io/using-nats/developer/connecting/creds.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigDeliverPolicy: {
			Default:     "all",
			Description: "DeliverPolicy defines where in the stream the connector should start receiving messages.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationInclusion{List: []string{"all", "new"}},
			},
		},
		ConfigDeliverSubject: {
			Default:     "",
			Description: "DeliverSubject specifies the JetStream consumer deliver subject.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigDurable: {
			Default:     "",
			Description: "Durable is the name of the Consumer, if set will make a consumer durable,\nallowing resuming consumption where left off.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigMaxReconnects: {
			Default:     "5",
			Description: "MaxReconnects sets the number of reconnect attempts that will be\ntried before giving up. If negative, then it will never give up\ntrying to reconnect.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{},
		},
		ConfigNkeyPath: {
			Default:     "",
			Description: "NKeyPath is the path to an NKey.\nSee https://docs.nats.io/using-nats/developer/connecting/nkey.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigReconnectWait: {
			Default:     "5s",
			Description: "ReconnectWait is the wait time between reconnect attempts.",
			Type:        config.ParameterTypeDuration,
			Validations: []config.Validation{},
		},
		ConfigStream: {
			Default:     "",
			Description: "Stream is the name of the Stream to be consumed.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigSubject: {
			Default:     "",
			Description: "Subject is the subject name.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigTlsClientCertPath: {
			Default:     "",
			Description: "TLSClientCertPath is the path to a client certificate.\nFor more details see https://docs.nats.io/using-nats/developer/connecting/tls.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigTlsClientPrivateKeyPath: {
			Default:     "",
			Description: "TLSClientPrivateKeyPath is the path to a private key.\nFor more details see https://docs.nats.io/using-nats/developer/connecting/tls.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigTlsRootCACertPath: {
			Default:     "",
			Description: "TLSRootCACertPath is the path to a root CA certificate.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigUrls: {
			Default:     "",
			Description: "URLs defines connection URLs.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
