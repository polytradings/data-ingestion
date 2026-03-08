package natsadapter

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type CryptoHistoryProvider struct {
	js         nats.JetStreamContext
	streamName string
	subjectTpl string
	maxAge     time.Duration
}

func NewCryptoHistoryProvider(nc *nats.Conn, streamName string, subjectTpl string, maxAge time.Duration) (*CryptoHistoryProvider, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream unavailable: %w", err)
	}
	provider := &CryptoHistoryProvider{js: js, streamName: strings.TrimSpace(streamName), subjectTpl: strings.TrimSpace(subjectTpl), maxAge: maxAge}
	if provider.streamName == "" {
		provider.streamName = "CRYPTO_PRICES"
	}
	if provider.subjectTpl == "" {
		provider.subjectTpl = "crypto.prices.%s.v1"
	}
	if provider.maxAge <= 0 {
		provider.maxAge = 15 * time.Minute
	}
	if err := provider.ensureStream(); err != nil {
		return nil, err
	}
	return provider, nil
}

func (p *CryptoHistoryProvider) ensureStream() error {
	_, err := p.js.StreamInfo(p.streamName)
	if err == nil {
		return nil
	}
	if err != nats.ErrStreamNotFound {
		return fmt.Errorf("stream info %s: %w", p.streamName, err)
	}
	_, err = p.js.AddStream(&nats.StreamConfig{
		Name:      p.streamName,
		Subjects:  []string{strings.ReplaceAll(p.subjectTpl, "%s", "*")},
		Retention: nats.LimitsPolicy,
		Storage:   nats.FileStorage,
		MaxAge:    p.maxAge,
	})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already in use") {
		return fmt.Errorf("create stream %s: %w", p.streamName, err)
	}
	return nil
}

func (p *CryptoHistoryProvider) LoadTicks(ctx context.Context, symbol string, start time.Time, end time.Time) ([]*proto.CryptoPriceTick, error) {
	symbol = strings.ToLower(strings.TrimSpace(symbol))
	if symbol == "" || !start.Before(end) {
		return nil, nil
	}
	subject := fmt.Sprintf(p.subjectTpl, symbol)
	sub, err := p.js.SubscribeSync(subject,
		nats.BindStream(p.streamName),
		nats.StartTime(start),
		nats.ReplayInstant(),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		return nil, fmt.Errorf("subscribe history %s: %w", subject, err)
	}
	defer func() {
		if derr := sub.Drain(); derr != nil {
			log.Printf("crypto history drain failed subject=%s: %v", subject, derr)
		}
	}()

	out := make([]*proto.CryptoPriceTick, 0, 256)
	for {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		msg, err := sub.NextMsg(400 * time.Millisecond)
		if err != nil {
			if err == nats.ErrTimeout {
				break
			}
			return nil, fmt.Errorf("read history message: %w", err)
		}

		var tick proto.CryptoPriceTick
		if err := proto.UnmarshalCryptoPriceTick(msg.Data, &tick); err != nil {
			_ = msg.Ack()
			continue
		}
		tickTime := time.UnixMilli(tick.TimestampUnixMs).UTC()
		if tickTime.After(end) {
			_ = msg.Ack()
			break
		}
		out = append(out, &tick)
		_ = msg.Ack()
	}
	return out, nil
}
