package market_price_aggregator

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/application/services"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type MarketCreatedListener struct {
	conn       *nats.Conn
	aggregator *services.MarketPriceAggregator
	subject    string
}

func NewMarketCreatedListener(conn *nats.Conn, aggregator *services.MarketPriceAggregator, subject string) *MarketCreatedListener {
	return &MarketCreatedListener{
		conn:       conn,
		aggregator: aggregator,
		subject:    subject,
	}
}

func (l *MarketCreatedListener) Start(ctx context.Context) error {
	sub, err := l.conn.Subscribe(l.subject, func(msg *nats.Msg) {
		var market proto.MarketCreated
		err := proto.UnmarshalMarketCreated(msg.Data, &market)
		if err != nil {
			log.Printf("error unmarshaling market created: %v", err)
			return
		}

		err = l.aggregator.HandleMarketCreated(ctx, &market)
		if err != nil {
			log.Printf("error handling market created: %v", err)
		}
	})

	if err != nil {
		return err
	}

	log.Printf("listening for %s", l.subject)
	go func() {
		<-ctx.Done()
		sub.Unsubscribe()
	}()

	return nil
}

type CryptoPriceListener struct {
	conn       *nats.Conn
	aggregator *services.MarketPriceAggregator
	subject    string
}

func NewCryptoPriceListener(conn *nats.Conn, aggregator *services.MarketPriceAggregator, subject string) *CryptoPriceListener {
	return &CryptoPriceListener{
		conn:       conn,
		aggregator: aggregator,
		subject:    subject,
	}
}

func (l *CryptoPriceListener) Start(ctx context.Context) error {
	// Subscribe to crypto prices with wildcard matching
	subjects := []string{l.subject}

	for _, subject := range subjects {
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
	subject    string
}

func NewTokenPriceListener(conn *nats.Conn, aggregator *services.MarketPriceAggregator, subject string) *TokenPriceListener {
	return &TokenPriceListener{
		conn:       conn,
		aggregator: aggregator,
		subject:    subject,
	}
}

func (l *TokenPriceListener) Start(ctx context.Context) error {
	// Subscribe to all token prices with wildcard matching
	sub, err := l.conn.Subscribe(l.subject, func(msg *nats.Msg) {
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

	log.Printf("listening for %s", l.subject)
	go func() {
		<-ctx.Done()
		sub.Unsubscribe()
	}()

	return nil
}
