package natsadapter

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/proto"
)

const marketInfoChannelBuffer = 256

type ProtoSubscriber struct {
	nc *nats.Conn
}

func NewProtoSubscriber(nc *nats.Conn) *ProtoSubscriber {
	return &ProtoSubscriber{nc: nc}
}

func (s *ProtoSubscriber) SubscribeMarketInfo(ctx context.Context, subject string) (<-chan *proto.MarketInfo, error) {
	ch := make(chan *proto.MarketInfo, marketInfoChannelBuffer)

	sub, err := s.nc.Subscribe(subject, func(msg *nats.Msg) {
		var m proto.MarketInfo
		if err := proto.UnmarshalMarketInfo(msg.Data, &m); err != nil {
			log.Printf("nats subscriber: unmarshal MarketInfo failed subject=%s: %v", subject, err)
			return
		}
		select {
		case ch <- &m:
		case <-ctx.Done():
		}
	})
	if err != nil {
		return nil, fmt.Errorf("nats subscribe %s: %w", subject, err)
	}

	go func() {
		<-ctx.Done()
		// Drain waits for all in-flight callbacks to complete before closing the subscription,
		// ensuring no callbacks will send to ch after it is closed below.
		if err := sub.Drain(); err != nil {
			log.Printf("nats subscriber: drain failed subject=%s: %v", subject, err)
		}
		close(ch)
	}()

	return ch, nil
}
