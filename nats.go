package natsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	uuid "github.com/kthomas/go.uuid"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

// GetNatsConsumerConcurrency returns the environment-configured concurrency
// specified for consumers; useful for configuring the number of subscriptions
// to create per NATS connection
func GetNatsConsumerConcurrency() uint64 {
	return natsConsumerConcurrency
}

// GetNatsConnection establishes, caches and returns a new NATS connection
func GetNatsConnection(drainTimeout time.Duration) (conn *nats.Conn, err error) {
	clientID, err := uuid.NewV4()
	if err != nil {
		log.Warningf("Failed to generate client id for NATS connection; %s", err.Error())
		return nil, err
	}

	options := []nats.Option{
		nats.Name(fmt.Sprintf("%s-%s", natsClientPrefix, clientID.String())),
		nats.Token(natsToken),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(-1),
		nats.DrainTimeout(drainTimeout),
		// nats.AuthTokenHandler(func() string { })
		nats.ClosedHandler(func(_conn *nats.Conn) {
			log.Debugf("NATS connection %s closed", _conn.Opts.Name)
			if _conn != nil {
				lastErr := _conn.LastError()
				if lastErr != nil {
					log.Warningf("NATS connection %s closed; %s", _conn.Opts.Name, lastErr.Error())
				}
			}
		}),
		nats.DisconnectErrHandler(func(_conn *nats.Conn, _err error) {
			log.Debugf("NATS connection %s disconnected", _conn.Opts.Name)
			natsConnectionMutex.Lock()
			delete(natsConnections, _conn.Opts.Name)
			natsConnectionMutex.Unlock()
		}),
		nats.ReconnectHandler(func(_conn *nats.Conn) {
			log.Debugf("NATS connection reestablished; %s", _conn.Opts.Name)
		}),
		nats.DiscoveredServersHandler(func(_conn *nats.Conn) {
			log.Debugf("NATS connection discovered peers; %s", _conn.Opts.Name)
		}),
	}

	if os.Getenv("NATS_FORCE_TLS") == "true" {
		options[len(options)] = nats.Secure()
	} else {
		certificates := make([]tls.Certificate, 0)
		clientAuth := tls.NoClientCert
		nameToCertificate := map[string]*tls.Certificate{}
		var rootCAs *x509.CertPool
		var clientCAs *x509.CertPool

		options[len(options)] = nats.Secure(&tls.Config{
			Certificates:      certificates,
			ClientAuth:        clientAuth,
			ClientCAs:         clientCAs,
			NameToCertificate: nameToCertificate,
			RootCAs:           rootCAs,
		})
	}

	conn, err = nats.Connect(natsURL, options...)

	if err != nil {
		log.Warningf("NATS connection failed; %s", err.Error())
		return nil, err
	}

	log.Debugf("Caching NATS connection: %s", conn.Opts.Name)
	natsConnectionMutex.Lock()
	defer natsConnectionMutex.Unlock()
	natsConnections[conn.Opts.Name] = conn

	return conn, nil
}

// GetNatsStreamingConnection establishes, caches and returns a new NATS streaming connection;
// the underlying NATS connection will not be closed when the NATS streaming subsystem exits.
func GetNatsStreamingConnection(drainTimeout time.Duration, connectionLostHandler func(_ stan.Conn, reason error)) (sconn stan.Conn, err error) {
	natsStreamingConnectionMutex.Lock()
	defer natsStreamingConnectionMutex.Unlock()

	conn, err := GetNatsConnection(drainTimeout)
	if err != nil {
		log.Warningf("NATS connection failed; %s", err.Error())
		return nil, err
	}

	clientName := []byte(conn.Opts.Name)
	sClientID := fmt.Sprintf("%s-%s", natsClientPrefix, conn.Opts.Name)
	sconn, err = stan.Connect(natsClusterID,
		sClientID,
		stan.NatsConn(conn),
		stan.SetConnectionLostHandler(func(c stan.Conn, reason error) {
			log.Warningf("NATS streaming connection lost: %s; %s", c, reason.Error())
			_conn := c.NatsConn()
			_name := string(clientName)
			if _conn != nil && _conn.Opts.Name == _name {
				natsConnectionMutex.Lock()
				defer natsConnectionMutex.Unlock()
				if staleConn, staleConnOk := natsConnections[_name]; staleConnOk {
					delete(natsConnections, _name)
					natsConnectionMutex.Unlock()

					if !staleConn.IsClosed() && !staleConn.IsDraining() {
						log.Debugf("Attempting to drain NATS connection: %s", clientName)
						err = staleConn.Drain()
						if err != nil {
							log.Warningf("Failed to drain stale NATS connection: %s; %s", clientName, err.Error())
						}
					}
				}
			} else if !_conn.IsClosed() && !_conn.IsDraining() {
				log.Debugf("Attempting to drain NATS connection: %s", clientName)
				_conn.Drain()
			}

			if connectionLostHandler != nil {
				connectionLostHandler(c, reason)
			}
		}))
	if err != nil {
		log.Warningf("NATS streaming connection failed; %s", err.Error())
		return nil, err
	}

	return sconn, nil
}

// AttemptNack tries to Nack the given message if it meets basic time-based deadlettering criteria
func AttemptNack(conn *stan.Conn, msg *stan.Msg, timeout int64) {
	if ShouldDeadletter(msg, timeout) {
		log.Debugf("Nacking redelivered %d-byte message after %dms timeout: %s", msg.Size(), timeout, msg.Subject)
		Nack(conn, msg)
	}
}

// Nack the given NATS message
func Nack(conn *stan.Conn, msg *stan.Msg) error {
	if conn == nil || (*conn).NatsConn() == nil || (*conn).NatsConn().IsClosed() || (*conn).NatsConn().IsDraining() || (*conn).NatsConn().IsReconnecting() {
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
		log.Warningf("Failed to Nack %d-byte NATS message on subject: %s; publish failed: %s", msg.Size(), msg.Subject, err.Error())
	}
	return err
}

// ShouldDeadletter determines if a given message should be deadlettered
func ShouldDeadletter(msg *stan.Msg, deadletterTimeout int64) bool {
	return msg.Redelivered && time.Now().Unix()-msg.Timestamp >= deadletterTimeout
}
