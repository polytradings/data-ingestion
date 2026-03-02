package natsadapter

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type ProtoPublisher struct {
	nc *nats.Conn
}

func NewProtoPublisher(nc *nats.Conn) *ProtoPublisher {
	return &ProtoPublisher{nc: nc}
}

func (p *ProtoPublisher) PublishCryptoPriceTick(ctx context.Context, subject string, tick *proto.CryptoPriceTick) error {
	payload, err := proto.MarshalCryptoPriceTick(tick)
	if err != nil {
		return fmt.Errorf("marshal protobuf: %w", err)
	}
	return p.publishPayload(subject, payload)
}

func (p *ProtoPublisher) PublishTokenPriceTick(ctx context.Context, subject string, tick *proto.TokenPriceTick) error {
	payload, err := proto.MarshalTokenPriceTick(tick)
	if err != nil {
		return fmt.Errorf("marshal protobuf: %w", err)
	}
	return p.publishPayload(subject, payload)
}

func (p *ProtoPublisher) PublishMarketCreated(ctx context.Context, subject string, market *proto.MarketCreated) error {
	payload, err := proto.MarshalMarketCreated(market)
	if err != nil {
		return fmt.Errorf("marshal protobuf: %w", err)
	}
	return p.publishPayload(subject, payload)
}

func (p *ProtoPublisher) PublishMarketDiscovered(ctx context.Context, subject string, market *proto.MarketDiscovered) error {
	payload, err := proto.MarshalMarketDiscovered(market)
	if err != nil {
		return fmt.Errorf("marshal protobuf: %w", err)
	}
	return p.publishPayload(subject, payload)
}

func (p *ProtoPublisher) PublishMarketAggregatedPrice(ctx context.Context, subject string, price *proto.MarketAggregatedPrice) error {
	payload, err := proto.MarshalMarketAggregatedPrice(price)
	if err != nil {
		return fmt.Errorf("marshal protobuf: %w", err)
	}
	return p.publishPayload(subject, payload)
}

func (p *ProtoPublisher) publishPayload(subject string, payload []byte) error {
	msg := &nats.Msg{Subject: subject, Data: payload}
	if err := p.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("nats publish: %w", err)
	}
	return nil
}
