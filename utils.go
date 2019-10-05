package natsutil

import (
	"fmt"
	"sync"
	"time"

	stan "github.com/nats-io/stan.go"
)

var sharedNatsStreamingConnectionMutex sync.Mutex

// EstablishSharedNatsStreamingConnection establishes or reestablishes the default shared NATS streaming connection
func EstablishSharedNatsStreamingConnection() error {
	if IsSharedNatsStreamingConnectionValid() {
		return nil
	}

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

// GetSharedNatsStreamingConnection retrieves the default shared NATS streaming connection
func GetSharedNatsStreamingConnection() (stan.Conn, error) {
	if sharedNatsStreamingConnection != nil {
		conn := sharedNatsStreamingConnection.NatsConn()
		if conn != nil && !conn.IsClosed() && !conn.IsDraining() && !conn.IsReconnecting() {
			return sharedNatsStreamingConnection, nil
		}
	}

	err := EstablishSharedNatsStreamingConnection()
	if err != nil {
		log.Warningf("Failed to establish shared NATS streaming connection; %s", err.Error())
		return sharedNatsStreamingConnection, err
	}
	return sharedNatsStreamingConnection, nil
}

// NatsPublish publishes a NATS message using the default shared NATS streaming environment
func NatsPublish(subject string, msg []byte) error {
	natsConnection, err := GetSharedNatsStreamingConnection()
	if err != nil {
		log.Warningf("Failed to retrieve shared NATS streaming connection for publish; %s", err.Error())
		return err
	}
	return natsConnection.Publish(subject, msg)
}

// NatsPublishAsync asynchronously publishes a NATS message using the default shared NATS streaming connection
func NatsPublishAsync(subject string, msg []byte) (*string, error) {
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

// AttemptNack tries to Nack the given NATS message if it meets basic time-based deadlettering criteria, using the default shared NATS streaming connection
func AttemptNack(msg *stan.Msg, timeout int64) {
	timeoutMillis := timeout / 1000 / 1000
	if ShouldDeadletter(msg, timeoutMillis) {
		log.Debugf("Nacking redelivered %d-byte message after %dms timeout: %s", msg.Size(), timeoutMillis, msg.Subject)
		Nack(msg)
	}
}

// Nack the given NATS message using the default shared NATS streaming connection
func Nack(msg *stan.Msg) error {
	if !IsSharedNatsStreamingConnectionValid() {
		err := fmt.Errorf("Cannot Nack %d-byte NATS message on subject: %s", msg.Size(), msg.Subject)
		log.Warning(err.Error())
		return err
	}
	_, err := sharedNatsStreamingConnection.PublishAsync(natsDeadLetterSubject, msg.Data, func(_ string, err error) {
		if err == nil {
			err = msg.Ack()
			if err == nil {
				log.Debugf("Nacked %d-byte NATS message on subject: %s", msg.Size(), msg.Subject)
			} else {
				log.Warningf("Failed to Nack NATS message which was successfully dead-lettered: %s", err.Error())
			}
		}
	})
	if err != nil {
		log.Warningf("Failed to Nack %d-byte NATS message on subject: %s; publish failed: %s", msg.Size(), msg.Subject, err.Error())
	}
	return err
}

// ShouldDeadletter determines if a given message should be deadlettered by converting the
// given message timestamp and deadletterTimeout values from nanosecond and millisecond
// resolutions, respectively, to seconds and comparing against current UTC time
func ShouldDeadletter(msg *stan.Msg, deadletterTimeoutMillis int64) bool {
	return msg.Redelivered && time.Now().UTC().Unix()-(msg.Timestamp/1000/1000/1000) >= (deadletterTimeoutMillis/1000)
}

// IsSharedNatsStreamingConnectionValid returns true if the default NATS streaming
func IsSharedNatsStreamingConnectionValid() bool {
	return !(sharedNatsStreamingConnection == nil ||
		sharedNatsStreamingConnection.NatsConn() == nil ||
		sharedNatsStreamingConnection.NatsConn().IsClosed() ||
		sharedNatsStreamingConnection.NatsConn().IsDraining() ||
		sharedNatsStreamingConnection.NatsConn().IsReconnecting())
}

func stringOrNil(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}
