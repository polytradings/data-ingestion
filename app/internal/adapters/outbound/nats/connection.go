package natsadapter

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func Connect(url string) (*nats.Conn, error) {
	nc, err := nats.Connect(
		url,
		nats.Name("data-ingestion"),
		nats.Timeout(10*time.Second),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}
	return nc, nil
}
