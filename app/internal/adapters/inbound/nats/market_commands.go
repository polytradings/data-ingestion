package natsinbound

import (
	"context"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/domain"
	"github.com/polytradings/data-ingestion/internal/ports"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type MarketCommandConsumer struct {
	nc       *nats.Conn
	subject  string
	registry ports.MarketRegistry
}

func NewMarketCommandConsumer(nc *nats.Conn, subject string, registry ports.MarketRegistry) *MarketCommandConsumer {
	return &MarketCommandConsumer{nc: nc, subject: subject, registry: registry}
}

func (c *MarketCommandConsumer) Start(ctx context.Context) error {
	sub, err := c.nc.Subscribe(c.subject, func(msg *nats.Msg) {
		var command proto.MarketTrackCommand
		if err := proto.UnmarshalMarketTrackCommand(msg.Data, &command); err != nil {
			log.Printf("failed to decode market command: %v", err)
			return
		}
		c.handle(command)
	})
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()

	return nil
}

func (c *MarketCommandConsumer) handle(command proto.MarketTrackCommand) {
	action := strings.ToUpper(strings.TrimSpace(command.Action))
	switch action {
	case "UPSERT":
		if strings.TrimSpace(command.MarketId) == "" {
			log.Printf("ignored UPSERT command without market_id")
			return
		}
		c.registry.Upsert(domain.MarketTokens{
			MarketID:    command.MarketId,
			UpTokenID:   command.UpTokenId,
			DownTokenID: command.DownTokenId,
		})
		log.Printf("market upserted market_id=%s", command.MarketId)
	case "REMOVE":
		if strings.TrimSpace(command.MarketId) == "" {
			log.Printf("ignored REMOVE command without market_id")
			return
		}
		c.registry.Delete(command.MarketId)
		log.Printf("market removed market_id=%s", command.MarketId)
	default:
		log.Printf("unknown market command action=%q", action)
	}
}
