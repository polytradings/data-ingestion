package market_price_aggregator

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/application/services"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type MarketDiscoveredListener struct {
	conn       *nats.Conn
	aggregator *services.MarketPriceAggregator
}

func NewMarketDiscoveredListener(conn *nats.Conn, aggregator *services.MarketPriceAggregator) *MarketDiscoveredListener {
	return &MarketDiscoveredListener{
		conn:       conn,
		aggregator: aggregator,
	}
}

func (l *MarketDiscoveredListener) Start(ctx context.Context) error {
	sub, err := l.conn.Subscribe("market.discovered", func(msg *nats.Msg) {
		var market proto.MarketDiscovered
		err := proto.UnmarshalMarketDiscovered(msg.Data, &market)
		if err != nil {
			log.Printf("error unmarshaling market discovered: %v", err)
			return
		}

		err = l.aggregator.HandleMarketDiscovered(ctx, &market)
		if err != nil {
			log.Printf("error handling market discovered: %v", err)
		}
	})

	if err != nil {
		return err
	}

	log.Println("listening for market.discovered events")
	go func() {
		<-ctx.Done()
		sub.Unsubscribe()
	}()

	return nil
}

type CryptoPriceListener struct {
	conn       *nats.Conn
	aggregator *services.MarketPriceAggregator
}

func NewCryptoPriceListener(conn *nats.Conn, aggregator *services.MarketPriceAggregator) *CryptoPriceListener {
	return &CryptoPriceListener{
		conn:       conn,
		aggregator: aggregator,
	}
}

func (l *CryptoPriceListener) Start(ctx context.Context) error {
	// Subscribe to crypto prices with wildcard matching (prices.crypto.*)
	subjects := []string{"prices.crypto.>"}

	for _, subject := range subjects {
		subject := subject
		_, err := l.conn.Subscribe(subject, func(msg *nats.Msg) {
			var cryptoPrice proto.CryptoPriceTick
			err := proto.UnmarshalCryptoPriceTick(msg.Data, &cryptoPrice)
			if err != nil {
				log.Printf("error unmarshaling crypto price from %s: %v", subject, err)
				return
			}

			err = l.aggregator.HandleCryptoPrice(ctx, &cryptoPrice)
			if err != nil {
				log.Printf("error handling crypto price: %v", err)
			}
		})

		if err != nil {
			return err
		}

		log.Printf("listening for %s", subject)
	}

	go func() {
		<-ctx.Done()
	}()

	return nil
}

type TokenPriceListener struct {
	conn       *nats.Conn
	aggregator *services.MarketPriceAggregator
}

func NewTokenPriceListener(conn *nats.Conn, aggregator *services.MarketPriceAggregator) *TokenPriceListener {
	return &TokenPriceListener{
		conn:       conn,
		aggregator: aggregator,
	}
}

func (l *TokenPriceListener) Start(ctx context.Context) error {
	// Subscribe to all token prices with wildcard matching (prices.bet-token.*)
	sub, err := l.conn.Subscribe("prices.bet-token.>", func(msg *nats.Msg) {
		var tokenPrice proto.TokenPriceTick
		err := proto.UnmarshalTokenPriceTick(msg.Data, &tokenPrice)
		if err != nil {
			log.Printf("error unmarshaling token price: %v", err)
			return
		}

		err = l.aggregator.HandleTokenPrice(ctx, &tokenPrice)
		if err != nil {
			log.Printf("error handling token price: %v", err)
		}
	})

	if err != nil {
		return err
	}

	log.Println("listening for prices.bet-token.>")
	go func() {
		<-ctx.Done()
		sub.Unsubscribe()
	}()

	return nil
}
