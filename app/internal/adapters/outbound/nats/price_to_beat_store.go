package natsadapter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type PriceToBeatKVStore struct {
	kv nats.KeyValue
}

func NewPriceToBeatKVStore(nc *nats.Conn, bucket string) (*PriceToBeatKVStore, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		bucket = "price_to_beat"
	}
	kv, err := js.KeyValue(bucket)
	if err != nil {
		if err == nats.ErrBucketNotFound {
			kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket, History: 1, TTL: 24 * time.Hour})
		}
		if err != nil {
			return nil, fmt.Errorf("jetstream key-value bucket: %w", err)
		}
	}
	return &PriceToBeatKVStore{kv: kv}, nil
}

func (s *PriceToBeatKVStore) Load(ctx context.Context, marketID string) (*proto.PriceToBeat, bool, error) {
	entry, err := s.kv.Get(s.key(marketID))
	if err == nats.ErrKeyNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("kv get: %w", err)
	}
	var out proto.PriceToBeat
	if err := proto.UnmarshalPriceToBeat(entry.Value(), &out); err != nil {
		return nil, false, fmt.Errorf("unmarshal price-to-beat: %w", err)
	}
	return &out, true, nil
}

func (s *PriceToBeatKVStore) Save(ctx context.Context, marketID string, state *proto.PriceToBeat, ttl time.Duration) error {
	payload, err := proto.MarshalPriceToBeat(state)
	if err != nil {
		return fmt.Errorf("marshal price-to-beat: %w", err)
	}
	_, err = s.kv.Put(s.key(marketID), payload)
	if err != nil {
		return fmt.Errorf("kv put: %w", err)
	}
	return nil
}

func (s *PriceToBeatKVStore) Delete(ctx context.Context, marketID string) error {
	if err := s.kv.Delete(s.key(marketID)); err != nil && err != nats.ErrKeyNotFound {
		return fmt.Errorf("kv delete: %w", err)
	}
	return nil
}

func (s *PriceToBeatKVStore) key(marketID string) string {
	value := strings.ToLower(strings.TrimSpace(marketID))
	value = strings.ReplaceAll(value, ".", "_")
	value = strings.ReplaceAll(value, "*", "_")
	value = strings.ReplaceAll(value, ">", "_")
	value = strings.ReplaceAll(value, "/", "_")
	return value
}

type NoopPriceToBeatStore struct{}

func NewNoopPriceToBeatStore() *NoopPriceToBeatStore { return &NoopPriceToBeatStore{} }

func (s *NoopPriceToBeatStore) Load(ctx context.Context, marketID string) (*proto.PriceToBeat, bool, error) {
	return nil, false, nil
}

func (s *NoopPriceToBeatStore) Save(ctx context.Context, marketID string, state *proto.PriceToBeat, ttl time.Duration) error {
	return nil
}

func (s *NoopPriceToBeatStore) Delete(ctx context.Context, marketID string) error { return nil }
