package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/adapters/inbound/market_price_aggregator"
	"github.com/polytradings/data-ingestion/internal/adapters/outbound"
	redisclient "github.com/polytradings/data-ingestion/internal/adapters/outbound/redis"
	"github.com/polytradings/data-ingestion/internal/application/services"
	"github.com/polytradings/data-ingestion/internal/proto"
)

func main() {
	// Load configuration from environment
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	// Connect to NATS
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("error connecting to nats: %v", err)
	}
	defer nc.Close()
	log.Printf("connected to nats: %s", natsURL)

	// Connect to Redis
	redisConn, err := redisclient.NewClient(redisURL)
	if err != nil {
		log.Fatalf("error connecting to redis: %v", err)
	}
	defer redisConn.Close()

	// Create publish channel
	pubChan := make(chan *proto.MarketAggregatedPrice, 100)

	// Create aggregator
	aggregator := services.NewMarketPriceAggregator(redisConn, pubChan)

	// Create listeners
	marketDiscoveredListener := market_price_aggregator.NewMarketDiscoveredListener(nc, aggregator)
	cryptoPriceListener := market_price_aggregator.NewCryptoPriceListener(nc, aggregator)
	tokenPriceListener := market_price_aggregator.NewTokenPriceListener(nc, aggregator)

	// Start listeners
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := marketDiscoveredListener.Start(ctx); err != nil {
		log.Fatalf("error starting market discovered listener: %v", err)
	}

	if err := cryptoPriceListener.Start(ctx); err != nil {
		log.Fatalf("error starting crypto price listener: %v", err)
	}

	if err := tokenPriceListener.Start(ctx); err != nil {
		log.Fatalf("error starting token price listener: %v", err)
	}

	// Create publisher
	publisher := outbound.NewMarketAggregatedPricePublisher(nc)

	// Start publisher loop
	go publisher.PublishLoop(pubChan)
	log.Println("publisher loop started as goroutine")

	log.Println("market price aggregator started")

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("shutting down...")
	cancel()
	close(pubChan)
}
