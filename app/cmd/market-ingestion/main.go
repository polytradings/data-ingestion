package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	natsadapter "github.com/polytradings/data-ingestion/internal/adapters/outbound/nats"
	"github.com/polytradings/data-ingestion/internal/adapters/outbound/polymarket"
	"github.com/polytradings/data-ingestion/internal/adapters/outbound/retry"
	"github.com/polytradings/data-ingestion/internal/application"
	"github.com/polytradings/data-ingestion/internal/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadMarketIngestionConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	nc, err := natsadapter.Connect(cfg.NATSURL)
	if err != nil {
		log.Fatalf("NATS connection error: %v", err)
	}
	defer nc.Close()

	publisher := natsadapter.NewProtoPublisher(nc)
	httpBackoff := retry.Backoff{
		InitialDelay: cfg.HTTPRetryInitialDelay,
		MaxDelay:     cfg.HTTPRetryMaxDelay,
		Multiplier:   cfg.HTTPRetryMultiplier,
	}
	provider := polymarket.NewPolymarketTokenProvider(cfg.PolymarketMarketLookupURL, httpBackoff, cfg.HTTPRetryMaxAttempts)

	uc := application.NewDiscoverMarketsUseCase(
		provider,
		publisher,
		cfg.Cryptos,
		cfg.MarketTypes,
		cfg.NATSMarketCreatedSubject,
		cfg.NATSMarketExpiredSubject,
		cfg.MarketDiscoverInterval,
	)

	log.Printf(
		"market-ingestion started cryptos=%d market_types=%v discover_interval=%s",
		len(cfg.Cryptos),
		cfg.MarketTypes,
		cfg.MarketDiscoverInterval,
	)
	if err := uc.Execute(ctx); err != nil {
		log.Fatalf("market-ingestion stopped with error: %v", err)
	}
}
