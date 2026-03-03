package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/polytradings/data-ingestion/internal/adapters/inbound/market_price_aggregator"
	natsadapter "github.com/polytradings/data-ingestion/internal/adapters/outbound/nats"
	redisclient "github.com/polytradings/data-ingestion/internal/adapters/outbound/redis"
	"github.com/polytradings/data-ingestion/internal/application/services"
	"github.com/polytradings/data-ingestion/internal/config"
	"github.com/polytradings/data-ingestion/internal/proto"
)

func main() {
	// Load configuration
	cfg, err := config.LoadMarketPriceAggregatorConfig()
	if err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	// Connect to NATS
	nc, err := natsadapter.Connect(cfg.NATSURL)
	if err != nil {
		log.Fatalf("error connecting to nats: %v", err)
	}
	defer nc.Close()
	log.Printf("connected to nats: %s", cfg.NATSURL)

	// Connect to Redis
	redisConn, err := redisclient.NewClient(cfg.RedisURL)
	if err != nil {
		log.Fatalf("error connecting to redis: %v", err)
	}
	defer redisConn.Close()
	log.Printf("connected to redis: %s", cfg.RedisURL)

	// Create publish channel
	pubChan := make(chan *proto.MarketAggregatedPrice, 100)

	// Create aggregator
	aggregator := services.NewMarketPriceAggregator(redisConn, pubChan)

	// Create listeners with configured subjects
	marketCreatedListener := market_price_aggregator.NewMarketCreatedListener(
		nc,
		aggregator,
		cfg.NATSMarketCreatedSubject,
	)
	cryptoPriceListener := market_price_aggregator.NewCryptoPriceListener(
		nc,
		aggregator,
		cfg.NATSCryptoPriceSubjectPattern,
	)
	tokenPriceListener := market_price_aggregator.NewTokenPriceListener(
		nc,
		aggregator,
		cfg.NATSTokenPriceSubjectPattern,
	)

	// Start listeners
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := marketCreatedListener.Start(ctx); err != nil {
		log.Fatalf("error starting market created listener: %v", err)
	}

	if err := cryptoPriceListener.Start(ctx); err != nil {
		log.Fatalf("error starting crypto price listener: %v", err)
	}

	if err := tokenPriceListener.Start(ctx); err != nil {
		log.Fatalf("error starting token price listener: %v", err)
	}

	// Create publisher
	publisher := natsadapter.NewProtoPublisher(nc)

	// Start publisher loop
	go func() {
		log.Println("[NATS] publisher loop started, waiting for messages...")
		for msg := range pubChan {
			log.Printf("[NATS] received aggregated price for market: %s", msg.MarketId)
			subject := cfg.NATSMarketAggregatedPriceSubject
			err := publisher.PublishMarketAggregatedPrice(ctx, subject, msg)
			if err != nil {
				log.Printf("[NATS] error publishing aggregated price: %v", err)
			} else {
				log.Printf("[NATS] successfully published to %s: market_id=%s crypto=%.8f up=%.8f down=%.8f",
					subject, msg.MarketId, msg.CryptoPrice, msg.UpTokenPrice, msg.DownTokenPrice)
			}
		}
		log.Println("[NATS] publisher loop ended")
	}()

	log.Println("market price aggregator started")

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("shutting down...")
	cancel()
	close(pubChan)
}
