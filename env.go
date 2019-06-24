package natsutil

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kthomas/go-logger"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

const defaultNatsDeadLetterSubject = "nats.deadletter"

var (
	log *logger.Logger

	natsConnections     map[string]*nats.Conn
	natsConnectionMutex sync.Mutex

	natsStreamingConnections     map[string]*stan.Conn
	natsStreamingConnectionMutex sync.Mutex

	natsClientPrefix        string
	natsClusterID           string
	natsConnectTimeout      time.Duration
	natsConsumerConcurrency uint64 // number of subscriptions to open per NATS connection
	natsDeadLetterSubject   string
	natsToken               string
	natsURL                 string
	natsStreamingURL        string

	natsForceTLS             bool
	natsTLSConfig            *tls.Config
	natsTLSCertificates      []tls.Certificate
	natsClientAuth           int
	natsNameToCertificate    map[string]*tls.Certificate
	natsRootCACertificates   *x509.CertPool
	natsClientCACertificates *x509.CertPool
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

	natsForceTLS = os.Getenv("NATS_FORCE_TLS") == "true"
	if !natsForceTLS {
		if os.Getenv("NATS_TLS_CERTIFICATES") != "" {
			natsTLSCertificatesJSON := os.Getenv("NATS_TLS_CERTIFICATES")
			tlsCertificatesMap := map[string]string{} // mapping private key path -> certificate path
			json.Unmarshal([]byte(natsTLSCertificatesJSON), &tlsCertificatesMap)

			natsTLSCertificates = make([]tls.Certificate, 0)
			for keyPath, certPath := range tlsCertificatesMap {
				keyFile, _ := os.Open(keyPath)
				keyBytes := []byte{}
				_, err := keyFile.Read(keyBytes)

				certFile, _ := os.Open(certPath)
				certBytes := []byte{}
				_, err = certFile.Read(certBytes)

				cert, err := tls.X509KeyPair(certBytes, keyBytes)
				if err != nil {
					log.Warningf("Failed to parse X509 keypair with keypath: %s; certpath: %s; %s", keyPath, certPath, err.Error())
				} else {
					natsTLSCertificates = append(natsTLSCertificates, cert)
				}
			}
		}

		if os.Getenv("NATS_CLIENT_AUTH") != "" {
			clientAuthStr := os.Getenv("NATS_CLIENT_AUTH")
			natsClientAuth, _ = strconv.Atoi(clientAuthStr)
		} else {
			natsClientAuth = int(tls.ClientAuthType(tls.NoClientCert))
		}

		if os.Getenv("NATS_NAME_TO_CERTIFICATE") != "" {
			nameToCertificateJSON := os.Getenv("NATS_NAME_TO_CERTIFICATE")
			nameToKeyPairMap := map[string]map[string]string{}
			json.Unmarshal([]byte(nameToCertificateJSON), &nameToKeyPairMap)

			natsNameToCertificate = map[string]*tls.Certificate{}
			for name, keyPair := range nameToKeyPairMap {
				for keyPath, certPath := range keyPair {
					keyFile, _ := os.Open(keyPath)
					keyBytes := []byte{}
					_, err := keyFile.Read(keyBytes)

					certFile, _ := os.Open(certPath)
					certBytes := []byte{}
					_, err = certFile.Read(certBytes)

					cert, err := tls.X509KeyPair(certBytes, keyBytes)
					if err != nil {
						log.Warningf("Failed to parse X509 keypair with keypath: %s; certpath: %s; %s", keyPath, certPath, err.Error())
					} else {
						natsNameToCertificate[name] = &cert
					}
				}
			}
		}

		if os.Getenv("NATS_ROOT_CA_CERTIFICATES") != "" {
			rootCACertificates := strings.Split(os.Getenv("NATS_ROOT_CA_CERTIFICATES"), ",")
			log.Debugf("Parsed root CA certificates: %s", rootCACertificates)
		}

		if os.Getenv("NATS_CLIENT_CA_CERTIFICATES") != "" {
			clientCACertificates := strings.Split(os.Getenv("NATS_CLIENT_CA_CERTIFICATES"), ",")
			log.Debugf("Parsed client CA certificates: %s", clientCACertificates)
		}

		if os.Getenv("NATS_CONNECT_TIMEOUT") != "" {
			timeout, err := time.ParseDuration(os.Getenv("NATS_CONNECT_TIMEOUT"))
			if err != nil {
				natsConnectTimeout = nats.DefaultTimeout
			} else {
				natsConnectTimeout = timeout
			}
		}

		if len(natsTLSCertificates) > 0 || natsClientAuth != 0 || natsRootCACertificates != nil || len(natsNameToCertificate) > 0 {
			// certificates := make([]tls.Certificate, 0)
			// clientAuth := tls.NoClientCert
			// nameToCertificate := map[string]*tls.Certificate{}
			// var rootCAs *x509.CertPool
			// var clientCAs *x509.CertPool

			natsTLSConfig = &tls.Config{
				Certificates:      natsTLSCertificates,
				ClientAuth:        tls.ClientAuthType(natsClientAuth),
				ClientCAs:         natsClientCACertificates,
				NameToCertificate: natsNameToCertificate,
				RootCAs:           natsRootCACertificates,
			}
		}
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
