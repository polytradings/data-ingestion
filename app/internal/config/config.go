package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/domain"
)

type CryptoIngestionConfig struct {
	NATSURL                   string
	NATSCryptoSubjectTemplate string
	Platform                  string
	Cryptos                   []domain.Crypto
	Symbols                   []string

	BinanceWSURL string

	PolymarketWSURL string

	WebSocketRetryInitialDelay time.Duration
	WebSocketRetryMaxDelay     time.Duration
	WebSocketRetryMultiplier   float64
}

type TokenIngestionConfig struct {
	NATSURL                  string
	NATSTokenSubjectTemplate string
	NATSMarketCreatedSubject string

	MarketDiscoverInterval time.Duration

	PolymarketMarketLookupURL string
	PolymarketMarketWSURL     string

	WebSocketRetryInitialDelay time.Duration
	WebSocketRetryMaxDelay     time.Duration
	WebSocketRetryMultiplier   float64

	HTTPRetryMaxAttempts  int
	HTTPRetryInitialDelay time.Duration
	HTTPRetryMaxDelay     time.Duration
	HTTPRetryMultiplier   float64

	Cryptos     []domain.Crypto
	MarketTypes []domain.MarketType
}

func LoadCryptoIngestionConfig() (CryptoIngestionConfig, error) {
	platform := strings.ToLower(getOrDefault("INGESTION_PLATFORM", "binance"))

	cryptos, err := parseCryptos(getOrDefault("CRYPTO_SYMBOLS", "btc:bitcoin:usdc"))
	if err != nil {
		return CryptoIngestionConfig{}, fmt.Errorf("CRYPTO_SYMBOLS invalid: %w", err)
	}

	symbols := tradingSymbolsFromCryptos(cryptos, platform)

	cfg := CryptoIngestionConfig{
		NATSURL:                   getOrDefault("NATS_URL", "nats://localhost:4222"),
		NATSCryptoSubjectTemplate: getOrDefault("NATS_SUBJECT_CRYPTO_PRICE_TEMPLATE", "prices.crypto.%s.v1"),
		Platform:                  platform,
		Cryptos:                   cryptos,
		Symbols:                   symbols,
		BinanceWSURL:              getOrDefault("BINANCE_WS_URL", "wss://fstream.binance.com/stream"),
		PolymarketWSURL:           getOrDefault("POLYMARKET_WS_URL", "wss://ws-live-data.polymarket.com"),

		WebSocketRetryInitialDelay: getDurationOrDefault("WEBSOCKET_RETRY_INITIAL_DELAY", 1*time.Second),
		WebSocketRetryMaxDelay:     getDurationOrDefault("WEBSOCKET_RETRY_MAX_DELAY", 30*time.Second),
		WebSocketRetryMultiplier:   getFloatOrDefault("WEBSOCKET_RETRY_MULTIPLIER", 2),
	}

	if cfg.Platform != "binance" && cfg.Platform != "polymarket" {
		return cfg, fmt.Errorf("INGESTION_PLATFORM must be 'binance' or 'polymarket'")
	}
	if len(cfg.Symbols) == 0 {
		return cfg, fmt.Errorf("no trading symbols configured")
	}
	if cfg.WebSocketRetryInitialDelay <= 0 {
		return cfg, fmt.Errorf("WEBSOCKET_RETRY_INITIAL_DELAY must be positive")
	}
	if cfg.WebSocketRetryMaxDelay <= 0 {
		return cfg, fmt.Errorf("WEBSOCKET_RETRY_MAX_DELAY must be positive")
	}
	if cfg.WebSocketRetryInitialDelay > cfg.WebSocketRetryMaxDelay {
		return cfg, fmt.Errorf("WEBSOCKET_RETRY_INITIAL_DELAY must be <= WEBSOCKET_RETRY_MAX_DELAY")
	}
	if cfg.WebSocketRetryMultiplier <= 1 {
		return cfg, fmt.Errorf("WEBSOCKET_RETRY_MULTIPLIER must be > 1")
	}

	return cfg, nil
}

func LoadTokenIngestionConfig() (TokenIngestionConfig, error) {
	discoverySeconds, err := strconv.Atoi(getOrDefault("TOKEN_MARKET_DISCOVERY_INTERVAL_SECONDS", "10"))
	if err != nil || discoverySeconds <= 0 {
		return TokenIngestionConfig{}, fmt.Errorf("TOKEN_MARKET_DISCOVERY_INTERVAL_SECONDS must be a positive integer")
	}

	marketTypes, err := parseMarketTypes(getOrDefault("TOKEN_MARKET_TYPES", "5,15,60"))
	if err != nil {
		return TokenIngestionConfig{}, err
	}

	cryptos, err := parseCryptos(getOrDefault("CRYPTO_SYMBOLS", "btc:bitcoin:usdc,eth:ethereum:usdc"))
	if err != nil {
		return TokenIngestionConfig{}, fmt.Errorf("CRYPTO_SYMBOLS invalid: %w", err)
	}

	cfg := TokenIngestionConfig{
		NATSURL:                  getOrDefault("NATS_URL", "nats://localhost:4222"),
		NATSTokenSubjectTemplate: getOrDefault("NATS_SUBJECT_BET_TOKEN_PRICE_TEMPLATE", "prices.bet-token.%s.v1"),
		NATSMarketCreatedSubject: getOrDefault("NATS_SUBJECT_MARKET_CREATED", "markets.created.v1"),
		MarketDiscoverInterval:   time.Duration(discoverySeconds) * time.Second,
		PolymarketMarketLookupURL: getOrDefault(
			"POLYMARKET_MARKET_LOOKUP_URL",
			"https://gamma-api.polymarket.com/markets",
		),
		PolymarketMarketWSURL: getOrDefault(
			"POLYMARKET_MARKET_WS_URL",
			"wss://ws-subscriptions-clob.polymarket.com/ws/market",
		),
		WebSocketRetryInitialDelay: getDurationOrDefault("WEBSOCKET_RETRY_INITIAL_DELAY", 1*time.Second),
		WebSocketRetryMaxDelay:     getDurationOrDefault("WEBSOCKET_RETRY_MAX_DELAY", 30*time.Second),
		WebSocketRetryMultiplier:   getFloatOrDefault("WEBSOCKET_RETRY_MULTIPLIER", 2),

		HTTPRetryMaxAttempts:  getIntOrDefault("HTTP_RETRY_MAX_ATTEMPTS", 5),
		HTTPRetryInitialDelay: getDurationOrDefault("HTTP_RETRY_INITIAL_DELAY", 500*time.Millisecond),
		HTTPRetryMaxDelay:     getDurationOrDefault("HTTP_RETRY_MAX_DELAY", 5*time.Second),
		HTTPRetryMultiplier:   getFloatOrDefault("HTTP_RETRY_MULTIPLIER", 2),

		Cryptos:     cryptos,
		MarketTypes: marketTypes,
	}

	if cfg.WebSocketRetryInitialDelay <= 0 {
		return cfg, fmt.Errorf("WEBSOCKET_RETRY_INITIAL_DELAY must be positive")
	}
	if cfg.WebSocketRetryMaxDelay <= 0 {
		return cfg, fmt.Errorf("WEBSOCKET_RETRY_MAX_DELAY must be positive")
	}
	if cfg.WebSocketRetryInitialDelay > cfg.WebSocketRetryMaxDelay {
		return cfg, fmt.Errorf("WEBSOCKET_RETRY_INITIAL_DELAY must be <= WEBSOCKET_RETRY_MAX_DELAY")
	}
	if cfg.WebSocketRetryMultiplier <= 1 {
		return cfg, fmt.Errorf("WEBSOCKET_RETRY_MULTIPLIER must be > 1")
	}
	if cfg.HTTPRetryMaxAttempts <= 0 {
		return cfg, fmt.Errorf("HTTP_RETRY_MAX_ATTEMPTS must be positive")
	}
	if cfg.HTTPRetryInitialDelay <= 0 {
		return cfg, fmt.Errorf("HTTP_RETRY_INITIAL_DELAY must be positive")
	}
	if cfg.HTTPRetryMaxDelay <= 0 {
		return cfg, fmt.Errorf("HTTP_RETRY_MAX_DELAY must be positive")
	}
	if cfg.HTTPRetryInitialDelay > cfg.HTTPRetryMaxDelay {
		return cfg, fmt.Errorf("HTTP_RETRY_INITIAL_DELAY must be <= HTTP_RETRY_MAX_DELAY")
	}
	if cfg.HTTPRetryMultiplier <= 1 {
		return cfg, fmt.Errorf("HTTP_RETRY_MULTIPLIER must be > 1")
	}

	return cfg, nil
}

func getOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getDurationOrDefault(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func getFloatOrDefault(key string, fallback float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func getIntOrDefault(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseCryptos(raw string) ([]domain.Crypto, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("cannot be empty")
	}

	items := strings.Split(raw, ",")
	out := make([]domain.Crypto, 0, len(items))
	for _, item := range items {
		chunks := strings.Split(strings.TrimSpace(item), ":")
		if len(chunks) != 3 {
			return nil, fmt.Errorf("invalid crypto format %q, expected min:full:convert_to", item)
		}
		crypto := domain.Crypto{
			MinName:   strings.ToLower(strings.TrimSpace(chunks[0])),
			FullName:  strings.ToLower(strings.TrimSpace(chunks[1])),
			ConvertTo: strings.ToLower(strings.TrimSpace(chunks[2])),
		}
		if crypto.MinName == "" || crypto.FullName == "" || crypto.ConvertTo == "" {
			return nil, fmt.Errorf("invalid crypto format %q, expected min:full:convert_to", item)
		}
		out = append(out, crypto)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("cannot be empty")
	}
	return out, nil
}

func parseMarketTypes(raw string) ([]domain.MarketType, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("TOKEN_MARKET_TYPES cannot be empty")
	}

	items := strings.Split(raw, ",")
	out := make([]domain.MarketType, 0, len(items))
	seen := map[domain.MarketType]struct{}{}

	for _, item := range items {
		value, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil {
			return nil, fmt.Errorf("invalid market type %q", item)
		}
		mt := domain.MarketType(value)
		if mt != domain.MarketTypeFiveMinutes &&
			mt != domain.MarketTypeFifteenMinutes &&
			mt != domain.MarketTypeSixtyMinutes {
			return nil, fmt.Errorf("unsupported market type %d (allowed: 5,15,60)", value)
		}
		if _, ok := seen[mt]; ok {
			continue
		}
		seen[mt] = struct{}{}
		out = append(out, mt)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("TOKEN_MARKET_TYPES cannot be empty")
	}
	return out, nil
}

func tradingSymbolsFromCryptos(cryptos []domain.Crypto, platform string) []string {
	if len(cryptos) == 0 {
		return nil
	}

	out := make([]string, 0, len(cryptos))
	for _, crypto := range cryptos {
		base := strings.ToUpper(strings.TrimSpace(crypto.MinName))
		if base == "" {
			continue
		}

		switch platform {
		case "binance":
			quote := strings.ToUpper(strings.TrimSpace(crypto.ConvertTo))
			if quote == "" {
				quote = "USDT"
			}
			out = append(out, base+quote)
		default:
			out = append(out, base)
		}
	}
	return out
}
