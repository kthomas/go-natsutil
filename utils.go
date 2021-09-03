package natsutil

import (
	"sync"

	nats "github.com/nats-io/nats.go"
)

var sharedNatsConnectionMutex sync.Mutex
var sharedJetstreamConnectionMutex sync.Mutex

// EstablishSharedNatsConnection establishes or reestablishes the default shared NATS connection
func EstablishSharedNatsConnection(jwt *string) error {
	if IsSharedNatsConnectionValid() {
		return nil
	}

	sharedNatsConnectionMutex.Lock()
	defer sharedNatsConnectionMutex.Unlock()

	natsJWT := jwt
	if natsJWT == nil {
		natsJWT = natsDefaultBearerJWT
	}

	natsConnection, err := GetNatsConnection(natsURL, sharedJetstreamConnectionDrainTimeout, natsJWT)
	if err != nil {
		log.Warningf("Failed to establish shared NATS streaming connection; %s", err.Error())
		return err
	}
	sharedNatsConnection = natsConnection
	return nil
}

// GetSharedNatsConnection retrieves the default shared NATS connection
func GetSharedNatsConnection(jwt *string) (*nats.Conn, error) {
	if sharedNatsConnection != nil {
		if !sharedNatsConnection.IsClosed() && !sharedNatsConnection.IsDraining() && !sharedNatsConnection.IsReconnecting() {
			return sharedNatsConnection, nil
		}
	}

	natsJWT := jwt
	if natsJWT == nil {
		natsJWT = natsDefaultBearerJWT
	}

	err := EstablishSharedNatsConnection(jwt)
	if err != nil {
		log.Warningf("Failed to establish shared NATS connection; %s", err.Error())
		return sharedNatsConnection, err
	}
	return sharedNatsConnection, nil
}

// NatsPublish publishes a NATS message using the default shared NATS connection
func NatsPublish(subject string, msg []byte) error {
	natsConnection, err := GetSharedNatsConnection(natsDefaultBearerJWT)
	if err != nil {
		log.Warningf("Failed to retrieve shared NATS connection for publish; %s", err.Error())
		return err
	}
	return natsConnection.Publish(subject, msg)
}

// NatsPublishRequest publishes a NATS message using the default shared NATS connection
func NatsPublishRequest(subject, reply string, msg []byte) error {
	natsConnection, err := GetSharedNatsConnection(natsDefaultBearerJWT)
	if err != nil {
		log.Warningf("Failed to retrieve shared NATS connection for publishing request; %s", err.Error())
		return err
	}
	return natsConnection.PublishRequest(subject, reply, msg)
}

// NatsCreateStream creates a jetstream stream
func NatsCreateStream(name string, subjects []string) (*nats.StreamInfo, error) {
	js, err := GetNatsJetstreamContext(defaultNatsConnectionDrainTimeout, natsDefaultBearerJWT, defaultJetstreamMaxPending)
	if err != nil {
		log.Warningf("failed to retrieve shared NATS connection for stream management; %s", err.Error())
		return nil, err
	}

	var stream *nats.StreamInfo

	stream, err = js.StreamInfo(name)
	if err != nil {
		switch err {
		case nats.ErrStreamNotFound:
			log.Debugf("creating NATS stream %s with subjects: %s", name, subjects)
			stream, err = js.AddStream(&nats.StreamConfig{
				Name:     name,
				Subjects: subjects,
			})
			if err != nil {
				log.Warningf("failed to create NATS stream: %s; %s", name, err.Error())
				return stream, err
			}
		default:
			return nil, err
		}
	}

	return stream, nil
}

// NatsJetstreamPublish publishes a NATS jetstream message using the default shared NATS connection
func NatsJetstreamPublish(subject string, msg []byte) (*nats.PubAck, error) {
	js, err := GetNatsJetstreamContext(defaultNatsConnectionDrainTimeout, natsDefaultBearerJWT, defaultJetstreamMaxPending)
	if err != nil {
		log.Warningf("failed to retrieve shared NATS connection for asynchronous jetstream publish; %s", err.Error())
		return nil, err
	}
	return js.Publish(subject, msg)
}

// NatsJetstreamPublishAsync asynchronously publishes a NATS message using the default shared NATS jetstream context
func NatsJetstreamPublishAsync(subject string, msg []byte) (nats.PubAckFuture, error) {
	js, err := GetNatsJetstreamContext(defaultNatsConnectionDrainTimeout, natsDefaultBearerJWT, defaultJetstreamMaxPending)
	if err != nil {
		log.Warningf("failed to retrieve shared NATS connection for asynchronous jetstream publish; %s", err.Error())
		return nil, err
	}
	future, err := js.PublishAsync(subject, msg)
	if err != nil {
		log.Warningf("failed to asynchronously publish %d-byte NATS jetstream message; %s", len(msg), err.Error())
		return nil, err
	}
	return future, err
}

// IsSharedNatsConnectionValid returns true if the default NATS connection is valid for use
func IsSharedNatsConnectionValid() bool {
	return !(sharedNatsConnection == nil ||
		sharedNatsConnection.IsClosed() ||
		sharedNatsConnection.IsDraining() ||
		sharedNatsConnection.IsReconnecting())
}
