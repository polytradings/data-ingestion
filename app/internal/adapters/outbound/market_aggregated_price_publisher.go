package outbound

import (
	"log"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type MarketAggregatedPricePublisher struct {
	conn *nats.Conn
}

func NewMarketAggregatedPricePublisher(conn *nats.Conn) *MarketAggregatedPricePublisher {
	return &MarketAggregatedPricePublisher{
		conn: conn,
	}
}

func (p *MarketAggregatedPricePublisher) Publish(msg *proto.MarketAggregatedPrice) error {
	subject := "market.prices." + msg.MarketId

	payload, err := proto.MarshalMarketAggregatedPrice(msg)
	if err != nil {
		log.Printf("error marshaling aggregated price: %v", err)
		return err
	}

	err = p.conn.Publish(subject, payload)
	if err != nil {
		log.Printf("error publishing to %s: %v", subject, err)
		return err
	}

	return nil
}

func (p *MarketAggregatedPricePublisher) PublishLoop(pubChan <-chan *proto.MarketAggregatedPrice) {
	for msg := range pubChan {
		err := p.Publish(msg)
		if err != nil {
			log.Printf("error in publish loop: %v", err)
		}
	}
}
