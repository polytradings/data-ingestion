# Data Ingestion

A Go event-driven data ingestion layer that streams cryptocurrency prices and prediction market data via [NATS](https://nats.io/) using [Protocol Buffers](https://protobuf.dev/).

## Purpose

This project connects to external data sources (Binance and Polymarket) to:

1. **Track real-time crypto prices** — streams live BTC, ETH and other asset prices as price-tick events on NATS.
2. **Discover prediction markets** — scans Polymarket for active bet markets matched to tracked crypto assets and configurable timeframes (5 min, 15 min, 60 min).
3. **Stream bet-token prices** — publishes real-time UP/DOWN token prices for each active market.

Any downstream service can subscribe to NATS topics and consume the Protobuf messages using the schema defined in this repo.

---

## Architecture

![Architecture Diagram](docs/architecture_v1-2.png)

---

## Services

### 1. `crypto-ingestion` — Crypto Price Streamer

Connects to a price feed (Binance or Polymarket) and publishes live cryptocurrency prices to NATS.

- **Run one instance per platform** — Binance and Polymarket run as separate containers.
- **WebSocket with exponential backoff** — automatically reconnects on failure.
- **Filters zero prices** — only valid ticks are published.

**Key env vars:**

| Variable                            | Description                           | Example                              |
| ----------------------------------- | ------------------------------------- | ------------------------------------ |
| `INGESTION_PLATFORM`                | Data source platform                  | `binance` or `polymarket`            |
| `CRYPTO_SYMBOLS`                    | Symbols to track (`min:full:convert`) | `btc:bitcoin:usdc,eth:ethereum:usdc` |
| `NATS_SUBJECT_CRYPTO_PRICE_PATTERN` | NATS subject pattern                  | `crypto.prices.%s.v1`                |

---

### 2. `market-ingestion` — Market Discovery

Periodically discovers active Polymarket prediction markets and publishes lifecycle events to NATS.

- **Discovery loop** — polls Polymarket's API at a configurable interval.
- **Timeframe filters** — 5-minute, 15-minute, and 60-minute markets.
- **Asset matching** — only markets for tracked crypto assets are considered.
- **Market lifecycle** — checks for expired markets every second and publishes expiry events.
- Publishes `market.created.v1` when a new market is found.
- Publishes `market.expired.v1` when a market expires (same payload as `market.created.v1` but with `closed=true`).

**Key env vars:**

| Variable                           | Description                       | Example                |
| ---------------------------------- | --------------------------------- | ---------------------- |
| `MARKET_DISCOVERY_INTERVAL_SECONDS`| How often to scan for new markets | `10`                   |
| `TOKEN_MARKET_TYPES`               | Timeframe windows (minutes)       | `5,15,60`              |
| `NATS_SUBJECT_MARKET_CREATED`      | Subject for new-market events     | `market.created.v1`    |
| `NATS_SUBJECT_MARKET_EXPIRED`      | Subject for expired-market events | `market.expired.v1`    |

---

### 3. `token-ingestion` — Token Price Streamer

Listens for market lifecycle events via NATS and streams UP/DOWN token prices for each active market.

- **Event-driven** — subscribes to `market.created.v1` and `market.expired.v1` to maintain active market registry in memory.
- **Dynamic subscriptions** — subscribes/unsubscribes from token price WebSocket streams as markets open and close.
- **WebSocket with exponential backoff** — automatically reconnects on failure.

**Key env vars:**

| Variable                           | Description                                | Example              |
| ---------------------------------- | ------------------------------------------ | -------------------- |
| `NATS_SUBJECT_MARKET_CREATED`      | Subject to consume new-market events       | `market.created.v1`  |
| `NATS_SUBJECT_MARKET_EXPIRED`      | Subject to consume expired-market events   | `market.expired.v1`  |
| `NATS_SUBJECT_TOKEN_PRICE_PATTERN` | Subject pattern for token prices           | `token.prices.%s.v1` |

---

## NATS Topics

| Subject                         | Direction | Message Type      | Description                                                               |
| ------------------------------- | --------- | ----------------- | ------------------------------------------------------------------------- |
| `crypto.prices.<asset>.v1`      | Published | `CryptoPriceTick` | Live price tick for a tracked crypto asset (e.g. `crypto.prices.btc.v1`) |
| `market.created.v1`             | Published | `MarketInfo`      | A new prediction market was discovered (`closed=false`)                   |
| `market.expired.v1`             | Published | `MarketInfo`      | A prediction market has expired (`closed=true`)                           |
| `token.prices.<market_slug>.v1` | Published | `TokenPriceTick`  | Live UP/DOWN token price for an active market                             |

---

## Protocol Buffers

All messages are serialized with Protocol Buffers. The schema lives at [`app/internal/proto/messages.proto`](app/internal/proto/messages.proto).

```protobuf
syntax = "proto3";

package ingestion;

// Published to: crypto.prices.<asset>.v1
// Live cryptocurrency price tick from an exchange or data provider.
message CryptoPriceTick {
  string source            = 1; // "binance" | "polymarket"
  string symbol            = 2; // e.g. "btc"
  double price             = 3;
  int64  timestamp_unix_ms = 4;
}

// Published to: token.prices.<market_slug>.v1
// Price tick for a single UP or DOWN prediction-market token.
message TokenPriceTick {
  string source            = 1; // "polymarket"
  string market_id         = 2;
  string condition_id      = 3;
  string token_id          = 4;
  string side              = 5; // "UP" | "DOWN"
  double price             = 6;
  int64  timestamp_unix_ms = 7;
}

// Published to: market.created.v1 (closed=false) and market.expired.v1 (closed=true)
// Fired when a new prediction market is discovered or expires.
message MarketInfo {
  string source                = 1; // "polymarket"
  string market_id             = 2;
  string condition_id          = 3;
  string crypto_symbol         = 4; // e.g. "btc"
  int32  timeframe_minutes     = 5; // 5 | 15 | 60
  string up_token_id           = 6;
  string down_token_id         = 7;
  int64  start_unix_ms         = 8;
  int64  end_unix_ms           = 9;
  int64  discovered_at_unix_ms = 10;
  bool   closed                = 11; // false = created, true = expired
}
```

---

## Running Locally

### Prerequisites

- [Go 1.22+](https://go.dev/dl/)
- [Docker](https://www.docker.com/) & Docker Compose
- A `.env` file (copy from `.env.example`)

### With Docker Compose (recommended)

```bash
cp .env.example .env
cd infra/docker
./compose-up.sh
./compose-up.sh -d
```

This starts all services plus NATS.

### WSL / Architecture (GOARCH)

Use the helper scripts in `infra/docker` (they detect architecture via `uname -m` and set `TARGETARCH` automatically):

```bash
cd infra/docker
./compose-up.sh
```

To only build images:

```bash
cd infra/docker
./compose-build.sh
```

If you prefer running Docker Compose directly (advanced usage), you can still force architecture manually:

```bash
# Force AMD64 (x86_64)
TARGETARCH=amd64 docker compose up --build

# Force ARM64
TARGETARCH=arm64 docker compose up --build
```

Quick architecture check (troubleshooting):

```bash
# Host (WSL/Linux)
uname -m

# Inside a running service container
docker compose exec crypto-ingestion-binance uname -m
```

Common outputs:

- `x86_64` => use `TARGETARCH=amd64`
- `aarch64` or `arm64` => use `TARGETARCH=arm64`

### Running Individual Services

```bash
cd app
go mod tidy

# Crypto ingestion — Binance
INGESTION_PLATFORM=binance go run ./cmd/crypto-ingestion

# Crypto ingestion — Polymarket
INGESTION_PLATFORM=polymarket CRYPTO_SYMBOLS=btc:bitcoin:usdc,eth:ethereum:usdc go run ./cmd/crypto-ingestion

# Market ingestion (discovers markets, publishes created/expired events)
TOKEN_MARKET_TYPES=5,15,60 go run ./cmd/market-ingestion

# Token ingestion (subscribes to market events, streams token prices)
go run ./cmd/token-ingestion
```

---

## Configuration Reference

Copy `.env.example` to `.env` and adjust the values for your environment.

| Variable                            | Default                                                | Description                                       |
| ----------------------------------- | ------------------------------------------------------ | ------------------------------------------------- |
| `NATS_URL`                          | `nats://localhost:4222`                                | NATS server connection URL                        |
| `INGESTION_PLATFORM`                | `polymarket`                                           | Price feed platform (`binance` \| `polymarket`)   |
| `CRYPTO_SYMBOLS`                    | `btc:bitcoin:usdc,eth:ethereum:usdc`                   | Assets to track (`min:full:convert_to`)           |
| `NATS_SUBJECT_CRYPTO_PRICE_PATTERN` | `crypto.prices.%s.v1`                                  | Subject pattern used by `crypto-ingestion`        |
| `NATS_SUBJECT_TOKEN_PRICE_PATTERN`  | `token.prices.%s.v1`                                   | Subject pattern used by `token-ingestion`         |
| `NATS_SUBJECT_MARKET_CREATED`       | `market.created.v1`                                    | Subject for publishing/consuming `MarketInfo` (created) |
| `NATS_SUBJECT_MARKET_EXPIRED`       | `market.expired.v1`                                    | Subject for publishing/consuming `MarketInfo` (expired) |
| `MARKET_DISCOVERY_INTERVAL_SECONDS` | `10`                                                   | Market discovery poll interval (seconds)          |
| `TOKEN_MARKET_TYPES`                | `5,15,60`                                              | Bet-market timeframes to watch (minutes)          |
| `POLYMARKET_WS_URL`                 | `wss://ws-live-data.polymarket.com`                    | Polymarket crypto WebSocket URL                   |
| `BINANCE_WS_URL`                    | `wss://fstream.binance.com/stream`                     | Binance WebSocket URL                             |
| `POLYMARKET_MARKET_LOOKUP_URL`      | `https://gamma-api.polymarket.com/markets`             | Polymarket market discovery API URL               |
| `POLYMARKET_MARKET_WS_URL`          | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Polymarket token-price WebSocket URL              |
| `WEBSOCKET_RETRY_INITIAL_DELAY`     | `500ms`                                                | Initial backoff delay for WebSocket reconnect     |
| `WEBSOCKET_RETRY_MAX_DELAY`         | `20s`                                                  | Maximum backoff delay for WebSocket reconnect     |
| `WEBSOCKET_RETRY_MULTIPLIER`        | `1.8`                                                  | Exponential factor for WebSocket backoff          |
| `HTTP_RETRY_MAX_ATTEMPTS`           | `8`                                                    | Maximum HTTP retry attempts                       |
| `HTTP_RETRY_INITIAL_DELAY`          | `300ms`                                                | Initial HTTP retry delay                          |
| `HTTP_RETRY_MAX_DELAY`              | `6s`                                                   | Maximum HTTP retry delay                          |
| `HTTP_RETRY_MULTIPLIER`             | `1.8`                                                  | Exponential factor for HTTP retry backoff         |
