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
		log.Printf("[NATS] error marshaling aggregated price: %v", err)
		return err
	}

	err = p.conn.Publish(subject, payload)
	if err != nil {
		log.Printf("[NATS] error publishing to %s: %v", subject, err)
		return err
	}

	log.Printf("[NATS] successfully published to %s: market_id=%s crypto=%.8f up=%.8f down=%.8f",
		subject, msg.MarketId, msg.CryptoPrice, msg.UpTokenPrice, msg.DownTokenPrice)

	return nil
}

func (p *MarketAggregatedPricePublisher) PublishLoop(pubChan <-chan *proto.MarketAggregatedPrice) {
	log.Println("[NATS] PublishLoop started, waiting for messages...")
	for msg := range pubChan {
		log.Printf("[NATS] received message from channel: market_id=%s", msg.MarketId)
		err := p.Publish(msg)
		if err != nil {
			log.Printf("[NATS] error in publish loop: %v", err)
		}
	}
	log.Println("[NATS] PublishLoop ended")
}
