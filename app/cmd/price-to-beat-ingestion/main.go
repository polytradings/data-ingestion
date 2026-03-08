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
	"github.com/polytradings/data-ingestion/internal/ports"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadPriceToBeatIngestionConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	nc, err := natsadapter.Connect(cfg.NATSURL)
	if err != nil {
		log.Fatalf("NATS connection error: %v", err)
	}
	defer nc.Close()

	publisher := natsadapter.NewProtoPublisher(nc)
	subscriber := natsadapter.NewProtoSubscriber(nc)
	httpBackoff := retry.Backoff{
		InitialDelay: cfg.HTTPRetryInitialDelay,
		MaxDelay:     cfg.HTTPRetryMaxDelay,
		Multiplier:   cfg.HTTPRetryMultiplier,
	}
	externalProvider := polymarket.NewPriceToBeatProvider(cfg.PriceToBeatBootstrapAPIURL, httpBackoff, cfg.HTTPRetryMaxAttempts)

	var stateStore ports.PriceToBeatStateStore = natsadapter.NewNoopPriceToBeatStore()
	jetstreamStore, err := natsadapter.NewPriceToBeatKVStore(nc, cfg.PriceToBeatJetStreamBucket)
	if err != nil {
		log.Printf("jetstream kv unavailable, falling back to in-memory noop store: %v", err)
	} else {
		stateStore = jetstreamStore
	}

	uc := application.NewTrackPriceToBeatUseCase(
		subscriber,
		subscriber,
		externalProvider,
		stateStore,
		publisher,
		cfg.NATSPriceToBeatSubjectPattern,
		cfg.NATSMarketCreatedSubject,
		cfg.NATSMarketExpiredSubject,
		cfg.NATSCryptoPriceSubjectPattern,
		cfg.PriceToBeatReconcileDelay,
		cfg.PriceToBeatPublishThresholdBps,
		cfg.PriceToBeatOpenGracePeriod,
	)

	log.Printf(
		"price-to-beat-ingestion started market_created_subject=%s market_expired_subject=%s crypto_subject_pattern=%s",
		cfg.NATSMarketCreatedSubject,
		cfg.NATSMarketExpiredSubject,
		cfg.NATSCryptoPriceSubjectPattern,
	)
	if err := uc.Execute(ctx); err != nil {
		log.Fatalf("price-to-beat-ingestion stopped with error: %v", err)
	}
}
