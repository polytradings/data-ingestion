package natsadapter

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/proto"
)

const (
	marketInfoChannelBuffer  = 256
	cryptoPriceChannelBuffer = 1024
)

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
		if err := sub.Drain(); err != nil {
			log.Printf("nats subscriber: drain failed subject=%s: %v", subject, err)
		}
		close(ch)
	}()

	return ch, nil
}

func (s *ProtoSubscriber) SubscribeCryptoPriceTick(ctx context.Context, subject string) (<-chan *proto.CryptoPriceTick, error) {
	ch := make(chan *proto.CryptoPriceTick, cryptoPriceChannelBuffer)

	sub, err := s.nc.Subscribe(subject, func(msg *nats.Msg) {
		var tick proto.CryptoPriceTick
		if err := proto.UnmarshalCryptoPriceTick(msg.Data, &tick); err != nil {
			log.Printf("nats subscriber: unmarshal CryptoPriceTick failed subject=%s: %v", msg.Subject, err)
			return
		}
		select {
		case ch <- &tick:
		case <-ctx.Done():
		}
	})
	if err != nil {
		return nil, fmt.Errorf("nats subscribe %s: %w", subject, err)
	}

	go func() {
		<-ctx.Done()
		if err := sub.Drain(); err != nil {
			log.Printf("nats subscriber: drain failed subject=%s: %v", subject, err)
		}
		close(ch)
	}()

	return ch, nil
}
