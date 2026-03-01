# Data Ingestion

Camada de ingestão em Go orientada a mensageria (NATS + protobuf).

## Serviços

- `market-ingestion`: uma instância por plataforma (`binance` ou `polymarket`) para publicar preço de cripto.
- `token-ingestion`: descobre mercados dinamicamente (5m/15m/60m), publica evento de novo mercado e streama preço dos tokens `UP`/`DOWN` ativos.

## Assuntos NATS (default)

- `prices.crypto.<asset>.v1`: ticks de cripto por ativo (ex.: `prices.crypto.btc.v1`).
- `markets.created.v1`: evento protobuf de novo mercado descoberto.
- `prices.bet-token.<market_slug>.v1`: ticks dos tokens `UP`/`DOWN` por mercado.

## Comandos

```bash
cd app
go mod tidy

go run ./cmd/market-ingestion
# ou
INGESTION_PLATFORM=polymarket go run ./cmd/market-ingestion
INGESTION_PLATFORM=binance;CRYPTO_SYMBOLS=btc:bitcoin:usdc go run ./cmd/market-ingestion
INGESTION_PLATFORM=polymarket;CRYPTO_SYMBOLS=btc:bitcoin:usdc,eth:ethereum:usdc go run ./cmd/market-ingestion

go run ./cmd/token-ingestion
```
