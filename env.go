package natsutil

import (
	"os"
	"strconv"
	"sync"

	"github.com/kthomas/go-logger"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

const defaultNatsDeadLetterSubject = "nats.deadletter"

var (
	log *logger.Logger

	natsConnections          map[string]*nats.Conn
	natsStreamingConnections map[string]*stan.Conn

	natsConnectionMutex          sync.Mutex
	natsStreamingConnectionMutex sync.Mutex

	natsClientPrefix        string
	natsClusterID           string
	natsConsumerConcurrency uint64 // number of subscriptions to open per NATS connection
	natsDeadLetterSubject   string
	natsToken               string
	natsURL                 string
	natsStreamingURL        string
)

func init() {
	lvl := os.Getenv("NATS_LOG_LEVEL")
	if lvl == "" {
		lvl = os.Getenv("LOG_LEVEL")
		if lvl == "" {
			lvl = "INFO"
		}
	}
	log = logger.NewLogger("go-natsutil", lvl, true)

	if os.Getenv("NATS_TOKEN") != "" {
		natsToken = os.Getenv("NATS_TOKEN")
	}

	if os.Getenv("NATS_URL") != "" {
		natsURL = os.Getenv("NATS_URL")
	}

	if os.Getenv("NATS_CLIENT_PREFIX") != "" {
		natsClientPrefix = os.Getenv("NATS_CLIENT_PREFIX")
	} else {
		natsClientPrefix = "go-natsutil"
	}

	if os.Getenv("NATS_CLUSTER_ID") != "" {
		natsClusterID = os.Getenv("NATS_CLUSTER_ID")
	}

	if os.Getenv("NATS_DEAD_LETTER_SUBJECT") != "" {
		natsDeadLetterSubject = os.Getenv("NATS_DEAD_LETTER_SUBJECT")
	} else {
		natsDeadLetterSubject = defaultNatsDeadLetterSubject
	}

	if os.Getenv("NATS_STREAMING_URL") != "" {
		natsStreamingURL = os.Getenv("NATS_STREAMING_URL")

		if os.Getenv("NATS_STREAMING_CONCURRENCY") != "" {
			concurrency, err := strconv.ParseUint(os.Getenv("NATS_STREAMING_CONCURRENCY"), 10, 8)
			if err == nil {
				natsConsumerConcurrency = concurrency
			} else {
				natsConsumerConcurrency = 1
			}
		}
	}

	natsConnections = map[string]*nats.Conn{}
	natsStreamingConnections = map[string]*stan.Conn{}
}
