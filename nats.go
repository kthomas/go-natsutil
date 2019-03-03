package natsutil

import (
	"fmt"

	uuid "github.com/kthomas/go.uuid"
	nats "github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
)

// GetNatsConsumerConcurrency returns the environment-configured concurrency
// specified for consumers
func GetNatsConsumerConcurrency() uint64 {
	return natsConsumerConcurrency
}

// GetNatsConnection returns the cached nats connection or establishes
// and caches it if it doesn't exist
func GetNatsConnection() *nats.Conn {
	if natsConnection == nil || natsConnection.IsClosed() {
		conn, err := nats.Connect(natsURL, nats.Token(natsToken))
		if err == nil {
			natsConnection = conn
		} else {
			log.Warningf("NATS connection failed; %s", err.Error())
		}
	}

	return natsConnection
}

// GetNatsStreamingConnection returns the cached nats streaming connection or
// establishes and caches it if it doesn't exist
func GetNatsStreamingConnection(connectionLostHandler func(_ stan.Conn, reason error)) *stan.Conn {
	if natsStreamingConnection == nil {
		clientID, err := uuid.NewV4()
		if err != nil {
			log.Warningf("Failed to generate client id for NATS streaming connection; %s", err.Error())
			return nil
		}

		natsConn, err := nats.Connect(natsStreamingURL, nats.Token(natsToken))
		if err != nil {
			log.Warningf("NATS connection failed; %s", err.Error())
			return nil
		}

		conn, err := stan.Connect(natsClusterID, fmt.Sprintf("%s-%s", natsClientPrefix, clientID.String()), stan.NatsConn(natsConn), stan.SetConnectionLostHandler(func(c stan.Conn, reason error) {
			natsStreamingConnection = nil
			if connectionLostHandler != nil {
				connectionLostHandler(c, reason)
			}
		}))
		if err == nil {
			natsStreamingConnection = &conn
		} else {
			log.Warningf("NATS streaming connection failed; %s", err.Error())
		}
	} else {
		conn := *natsStreamingConnection
		if conn.NatsConn() == nil || conn.NatsConn().IsClosed() {
			log.Debugf("NATS streaming closed; attempting to reconnect: %s", natsStreamingURL)
			natsStreamingConnection = nil
			return GetNatsStreamingConnection(connectionLostHandler)
		}
	}

	return natsStreamingConnection
}

// Nack the given NATS message
func Nack(conn *stan.Conn, msg *stan.Msg) error {
	if conn == nil || (*conn).NatsConn() == nil || (*conn).NatsConn().IsClosed() {
		err := fmt.Errorf("Cannot Nack %d-byte NATS message on subject: %s", msg.Size(), msg.Subject)
		log.Warning(err.Error())
		return err
	}
	_, err := (*conn).PublishAsync(natsDeadLetterSubject, msg.Data, func(_ string, err error) {
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
		log.Warningf("Failed to Nack %d-byte NATS message on subject: %s; publish failed: %s", msg.Size, msg.Subject, err.Error())
	}
	return err
}
