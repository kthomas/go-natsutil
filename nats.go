package natsutil

import (
	"fmt"
	"strings"
	"sync"
	"time"

	uuid "github.com/kthomas/go.uuid"
	nats "github.com/nats-io/nats.go"
)

// GetNatsConsumerConcurrency returns the environment-configured concurrency
// specified for consumers; useful for configuring the number of subscriptions
// to create per NATS connection
func GetNatsConsumerConcurrency() uint64 {
	return natsConsumerConcurrency
}

// GetNatsConnection establishes, caches and returns a new NATS connection
func GetNatsConnection(
	name, url string,
	drainTimeout time.Duration,
	jwt *string,
) (conn *nats.Conn, err error) {
	natsSecureOption := func(o *nats.Options) error {
		o.Secure = false
		return nil
	}
	if natsForceTLS {
		natsSecureOption = nats.Secure()
	} else if natsTLSConfig != nil {
		natsSecureOption = nats.Secure(natsTLSConfig)
	}

	clientUUID, _ := uuid.NewV4()
	clientName := fmt.Sprintf("%s-%s", name, clientUUID.String())

	options := []nats.Option{
		natsSecureOption,
		nats.Name(clientName),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(-1),
		nats.DrainTimeout(drainTimeout),
		nats.Timeout(natsConnectTimeout),
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
			log.Debugf("NATS connection %s diconnected", _conn.Opts.Name)
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
				log.Warningf("encountered asynchronous error on NATS connection: %s; subject: %s; %s", _conn.Opts.Name, subject, _err.Error())
			}
		}),
	}

	if natsToken != nil {
		options = append(options, nats.Token(*natsToken))
	}

	natsJWT := jwt
	if natsJWT == nil {
		natsJWT = natsDefaultBearerJWT
	}
	if natsJWT != nil {
		options = append(options, nats.UserJWT(
			func() (string, error) {
				return *natsJWT, nil
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

	return conn, nil
}

// GetNatsJetstreamContext establishes, caches and returns a new NATS jetstream context;
// the underlying NATS connection will not be closed when the NATS jetstream subsystem exits.
func GetNatsJetstreamContext(
	name string,
	drainTimeout time.Duration,
	jwt *string,
	maxPending int,
) (js nats.JetStreamContext, err error) {
	conn, err := GetNatsConnection(name, natsJetstreamURL, drainTimeout, jwt)
	if err != nil {
		log.Warningf("NATS connection failed; %s", err.Error())
		return nil, err
	}

	js, err = conn.JetStream(
		nats.PublishAsyncMaxPending(maxPending),
	)
	if err != nil {
		log.Warningf("failed to resolve NATS jetstream context; %s", err.Error())
		return nil, err
	}

	return js, nil
}

// RequireNatsJetstreamSubscription establishes, caches and returns a new NATS jetstream
// context using GetNatsJetstreamContext
func RequireNatsJetstreamSubscription(
	wg *sync.WaitGroup,
	drainTimeout time.Duration,
	subject, consumer, qgroup string,
	cb nats.MsgHandler,
	ackWait time.Duration,
	maxInFlight int,
	maxDeliveries int,
	jwt *string,
) (*nats.Subscription, error) {
	js, err := GetNatsJetstreamContext(consumer, drainTimeout, jwt, maxInFlight)
	if err != nil {
		log.Warningf("failed to require NATS jetstream context; %s", err.Error())
		return nil, err
	}

	subscription, err := js.QueueSubscribe(
		subject,
		qgroup,
		cb,
		nats.AckWait(ackWait),
		nats.Durable(strings.ReplaceAll(consumer, ".", "-")),
		nats.ManualAck(),
		nats.MaxAckPending(maxInFlight),
		nats.MaxDeliver(maxDeliveries),
	)

	if err != nil {
		log.Warningf("failed to subscribe to NATS jetstream subject: %s; %s", subject, err.Error())
		return nil, err
	}

	log.Debugf("subscribed to NATS jetsteam subject: %s", subject)
	return subscription, err
}
