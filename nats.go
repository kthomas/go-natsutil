package natsutil

import (
	"fmt"
	"sync"
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
func GetNatsConnection(url string, drainTimeout time.Duration, jwt *string) (conn *nats.Conn, err error) {
	clientID, err := uuid.NewV4()
	if err != nil {
		log.Warningf("Failed to generate client id for NATS connection; %s", err.Error())
		return nil, err
	}

	natsSecureOption := func(o *nats.Options) error {
		o.Secure = false
		return nil
	}
	if natsForceTLS {
		natsSecureOption = nats.Secure()
	} else if natsTLSConfig != nil {
		natsSecureOption = nats.Secure(natsTLSConfig)
	}

	options := []nats.Option{
		natsSecureOption,
		nats.Name(fmt.Sprintf("%s-%s", natsClientPrefix, clientID.String())),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(-1),
		nats.DrainTimeout(drainTimeout),
		nats.Timeout(natsConnectTimeout),
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
		}),
		nats.ReconnectHandler(func(_conn *nats.Conn) {
			log.Debugf("NATS connection reestablished: %s", _conn.Opts.Name)
		}),
		nats.DiscoveredServersHandler(func(_conn *nats.Conn) {
			log.Debugf("NATS connection discovered peers: %s", _conn.Opts.Name)
		}),
		nats.ErrorHandler(func(_conn *nats.Conn, sub *nats.Subscription, _err error) {
			if _err != nil {
				subject := ""
				if sub != nil {
					subject = sub.Subject
				}
				log.Warningf("Encountered asynchronous error on NATS connection: %s; subject: %s; %s", _conn.Opts.Name, subject, _err.Error())
			}
		}),
	}

	if natsToken != nil {
		options = append(options, nats.Token(*natsToken))
	}

	if jwt != nil {
		options = append(options, nats.UserJWT(
			func() (string, error) {
				return *jwt, nil
			},
			func([]byte) ([]byte, error) {
				return []byte{}, nil
			},
		))
	}

	conn, err = nats.Connect(url, options...)

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
func GetNatsStreamingConnection(drainTimeout time.Duration, jwt *string, connectionLostHandler func(_ stan.Conn, reason error)) (sconn *stan.Conn, err error) {
	conn, err := GetNatsConnection(natsStreamingURL, drainTimeout, jwt)
	if err != nil {
		log.Warningf("NATS connection failed; %s", err.Error())
		return nil, err
	}

	sClientUUID, err := uuid.NewV4()
	if err != nil {
		log.Warningf("Failed to generate client uuid for NATS streaming connection; %s", err.Error())
		return nil, err
	}

	clientName := []byte(conn.Opts.Name)
	sClientID := fmt.Sprintf("%s-%s-%s", natsClientPrefix, sClientUUID.String(), conn.Opts.Name)

	var connLostHandler func(stan.Conn, error)
	connLostHandler = func(c stan.Conn, reason error) {
		_name := string(clientName)
		log.Warningf("NATS streaming connection lost: %s; %s", _name, reason.Error())

		if natsConn, natsConnOk := natsConnections[_name]; natsConnOk {
			err := natsConn.Drain()
			if err != nil {
				log.Warningf("Failed to drain underlying NATS streaming connection: %s", _name)
			} else {
				log.Debugf("Drained underlying NATS streaming connection: %s", _name)
			}
		} else {
			log.Debugf("Failed to resolve underlying NATS streaming connection: %s", _name)
		}

		if _, natsStreamingConnOk := natsStreamingConnections[sClientID]; natsStreamingConnOk {
			natsStreamingConn, err := stan.Connect(natsClusterID,
				sClientID,
				stan.NatsConn(conn),
				stan.SetConnectionLostHandler(connLostHandler),
			)

			if err != nil {
				log.Warningf("Failed to reestablish NATS streaming connection: %s; %s", sClientID, err.Error())
			} else {
				log.Debugf("NATS streaming connection reestablished: %s", sClientID)
				natsStreamingConnectionMutex.Lock()
				natsStreamingConnections[sClientID] = &natsStreamingConn
				natsStreamingConnectionMutex.Unlock()
			}
		}

		if connectionLostHandler != nil {
			connectionLostHandler(c, reason)
		}
	}

	_sconn, err := stan.Connect(natsClusterID,
		sClientID,
		stan.NatsConn(conn),
		stan.SetConnectionLostHandler(connLostHandler),
	)
	if err != nil {
		log.Warningf("NATS streaming connection failed; %s", err.Error())
		return nil, err
	}
	sconn = &_sconn

	log.Debugf("Caching NATS streaming connection: %s", sClientID)
	natsStreamingConnectionMutex.Lock()
	defer natsStreamingConnectionMutex.Unlock()
	natsStreamingConnections[sClientID] = sconn

	return sconn, nil
}

// RequireNatsStreamingSubscription establishes, caches and returns a new NATS streaming connection
// using GetNatsStreamingConnection, subscribed to the durable subscription with the given parameters;
// it runs until it is told to exit (TODO: document signal handling)
func RequireNatsStreamingSubscription(wg *sync.WaitGroup, drainTimeout time.Duration, subject, qgroup string, cb stan.MsgHandler, ackWait time.Duration, maxInFlight int, jwt *string) {
	wg.Add(1)
	go func() {
		var subscribe func(_ stan.Conn, _ error)
		subscribe = func(_ stan.Conn, _ error) {
			var sconn *stan.Conn
			for {
				natsConnection, err := GetNatsStreamingConnection(drainTimeout, jwt, subscribe)
				if err != nil {
					log.Warningf("Failed to require NATS streaming connection; %s", err.Error())
					continue
				}
				sconn = natsConnection
				if sconn != nil {
					break
				}
			}

			subscription, err := (*sconn).QueueSubscribe(subject,
				qgroup,
				cb,
				stan.SetManualAckMode(),
				stan.AckWait(ackWait),
				stan.MaxInflight(maxInFlight),
				stan.DurableName(subject),
			)

			if err != nil {
				log.Warningf("Failed to subscribe to NATS subject: %s", subject)
				wg.Done()
				return
			}
			defer subscription.Close()
			log.Debugf("Subscribed to NATS subject: %s", subject)

			wg.Wait()
		}

		subscribe(nil, nil)
	}()
}
