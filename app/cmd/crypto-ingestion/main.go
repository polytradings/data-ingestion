package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/polytradings/data-ingestion/internal/adapters/outbound/binance"
	natsadapter "github.com/polytradings/data-ingestion/internal/adapters/outbound/nats"
	"github.com/polytradings/data-ingestion/internal/adapters/outbound/polymarket"
	"github.com/polytradings/data-ingestion/internal/adapters/outbound/retry"
	"github.com/polytradings/data-ingestion/internal/application"
	"github.com/polytradings/data-ingestion/internal/config"
	"github.com/polytradings/data-ingestion/internal/ports"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadCryptoIngestionConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	nc, err := natsadapter.Connect(cfg.NATSURL)
	if err != nil {
		log.Fatalf("NATS connection error: %v", err)
	}
	defer nc.Close()

	publisher := natsadapter.NewProtoPublisher(nc)
	wsBackoff := retry.Backoff{
		InitialDelay: cfg.WebSocketRetryInitialDelay,
		MaxDelay:     cfg.WebSocketRetryMaxDelay,
		Multiplier:   cfg.WebSocketRetryMultiplier,
	}

	var feed ports.CryptoPriceFeed
	switch cfg.Platform {
	case "binance":
		feed = binance.NewFeed(cfg.BinanceWSURL, wsBackoff)
	case "polymarket":
		feed = polymarket.NewFeed(cfg.PolymarketWSURL, wsBackoff)
	default:
		log.Fatalf("unsupported platform: %s", cfg.Platform)
	}

	uc := application.NewStreamCryptoPricesUseCase(feed, publisher, cfg.NATSCryptoSubjectPattern)

	log.Printf("market-ingestion started platform=%s symbols=%v", cfg.Platform, cfg.Symbols)
	if err := uc.Execute(ctx, cfg.Symbols); err != nil {
		log.Fatalf("market-ingestion stopped with error: %v", err)
	}
}
