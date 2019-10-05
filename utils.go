package natsutil

import (
	"sync"

	stan "github.com/nats-io/stan.go"
)

var sharedNatsStreamingConnectionMutex sync.Mutex

// EstablishSharedNATSStreamingConnection establishes (if conn is nil) or reestablishes the given NATS streaming connection
func EstablishSharedNATSStreamingConnection() error {
	sharedNatsStreamingConnectionMutex.Lock()
	defer sharedNatsStreamingConnectionMutex.Unlock()

	natsConnection, err := GetNatsStreamingConnection(sharedNatsStreamingConnectionDrainTimeout, nil)
	if err != nil {
		log.Warningf("Failed to establish shared NATS streaming connection; %s", err.Error())
		return err
	}
	sharedNatsStreamingConnection = *natsConnection
	return nil
}

// GetSharedNatsStreamingConnection retrieves the NATS streaming connection
func GetSharedNatsStreamingConnection() (stan.Conn, error) {
	if sharedNatsStreamingConnection != nil {
		conn := sharedNatsStreamingConnection.NatsConn()
		if conn != nil && !conn.IsClosed() && !conn.IsDraining() && !conn.IsReconnecting() {
			return sharedNatsStreamingConnection, nil
		}
	}

	err := EstablishSharedNATSStreamingConnection()
	if err != nil {
		log.Warningf("Failed to establish shared NATS streaming connection; %s", err.Error())
		return sharedNatsStreamingConnection, err
	}
	return sharedNatsStreamingConnection, nil
}

// NATSPublish publishes a NATS message using the configured NATS streaming environment
func NATSPublish(subject string, msg []byte) error {
	natsConnection, err := GetSharedNatsStreamingConnection()
	if err != nil {
		log.Warningf("Failed to retrieve shared NATS streaming connection for publish; %s", err.Error())
		return err
	}
	return natsConnection.Publish(subject, msg)
}

// NATSPublishAsync asynchronously publishes a NATS message using the configured NATS streaming environment
func NATSPublishAsync(subject string, msg []byte) (*string, error) {
	natsConnection, err := GetSharedNatsStreamingConnection()
	if err != nil {
		log.Warningf("Failed to retrieve shared NATS streaming connection for asynchronous publish; %s", err.Error())
		return nil, err
	}
	guid, err := natsConnection.PublishAsync(subject, msg, func(_ string, err error) {
		if err != nil {
			log.Warningf("Failed to asynchronously publish %d-byte NATS streaming message; %s", len(msg), err.Error())
		}
	})
	if err != nil {
		log.Warningf("Failed to asynchronously publish %d-byte NATS streaming message; %s", len(msg), err.Error())
		return nil, err
	}
	return stringOrNil(guid), err
}

func stringOrNil(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}
