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
	openPriceProvider := binance.NewOpenPriceProvider(cfg.PriceToBeatBinanceAPIURL, cfg.PriceToBeatBinanceQuoteSymbol, httpBackoff, cfg.HTTPRetryMaxAttempts)

	var stateStore ports.PriceToBeatStateStore = natsadapter.NewNoopPriceToBeatStore()
	jetstreamStore, err := natsadapter.NewPriceToBeatKVStore(nc, cfg.PriceToBeatJetStreamBucket)
	if err != nil {
		log.Printf("jetstream kv unavailable, falling back to in-memory noop store: %v", err)
	} else {
		stateStore = jetstreamStore
	}

	var historyProvider ports.CryptoPriceHistoryProvider = natsadapter.NewNoopCryptoHistoryProvider()
	jetstreamHistoryProvider, err := natsadapter.NewCryptoHistoryProvider(
		nc,
		cfg.PriceToBeatCryptoStreamName,
		cfg.NATSCryptoPriceSubjectPattern,
		cfg.PriceToBeatCryptoStreamMaxAge,
	)
	if err != nil {
		log.Printf("jetstream crypto history unavailable, falling back to no history provider: %v", err)
	} else {
		historyProvider = jetstreamHistoryProvider
	}

	uc := application.NewTrackPriceToBeatUseCase(
		subscriber,
		subscriber,
		externalProvider,
		openPriceProvider,
		historyProvider,
		stateStore,
		publisher,
		cfg.NATSPriceToBeatSubjectPattern,
		cfg.NATSMarketCreatedSubject,
		cfg.NATSMarketExpiredSubject,
		cfg.NATSCryptoPriceSubjectPattern,
		cfg.PriceToBeatReconcileDelay,
		cfg.PriceToBeatPublishThresholdBps,
		cfg.PriceToBeatOpenGracePeriod,
		cfg.PriceToBeatWindow,
		cfg.PriceToBeatUpdateCooldown,
		cfg.PriceToBeatPolymarketWeight,
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
