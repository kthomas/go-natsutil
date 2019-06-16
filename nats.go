package natsutil

import (
	"fmt"
	"time"

	uuid "github.com/kthomas/go.uuid"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

// GetNatsConsumerConcurrency returns the environment-configured concurrency
// specified for consumers
func GetNatsConsumerConcurrency() uint64 {
	return natsConsumerConcurrency
}

// GetNatsConnection returns the cached NATS connection or establishes
// and caches it if it doesn't exist
func GetNatsConnection(drainTimeout time.Duration) (conn *nats.Conn, err error) {
	clientID, err := uuid.NewV4()
	if err != nil {
		log.Warningf("Failed to generate client id for NATS connection; %s", err.Error())
		return nil, err
	}

	conn, err = nats.Connect(natsURL,
		nats.Name(fmt.Sprintf("%s-%s", natsClientPrefix, clientID.String())),
		nats.Token(natsToken),
		nats.Secure(), // FIXME-- do not use Secure() by itself; see RootCAs() or pass a proper TLS config into Secure()
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
	)

	if err != nil {
		log.Warningf("NATS connection failed; %s", err.Error())
		return nil, err
	} else {
		clientID, err := conn.GetClientID()
		if err != nil {
			log.Warningf("Failed to retrieve valid client id from NATS connection; %s", err.Error())
			return nil, err
		}

		natsConnectionMutex.Lock()
		defer natsConnectionMutex.Unlock()
		natsConnections[conn.Opts.Name] = conn

		log.Debugf("Caching NATS connection for client id: %s", clientID)
	}

	return conn, nil
}

// GetNatsStreamingConnection returns the cached NATS streaming connection or
// establishes and caches it if it doesn't exist
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
				if staleConn, staleConnOk := natsConnections[_name]; staleConnOk {
					natsConnectionMutex.Lock()
					delete(natsConnections, _name)
					natsConnectionMutex.Unlock()

					if !staleConn.IsClosed() {
						log.Debugf("Attempting to drain NATS connection: %s", clientName)
						err = staleConn.Drain()
						if err != nil {
							log.Warningf("Failed to drain stale NATS connection: %s; %s", clientName, err.Error())
						}
					}
				}
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
