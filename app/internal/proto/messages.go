package proto

import "fmt"

// message definitions for protobuf payloads; these are hand‑written
// rather than generated.  Marshaling/unmarshaling helpers live in
// codec.go so there is no dependency on the deprecated protobuf package.

type CryptoPriceTick struct {
	Source          string  `protobuf:"bytes,1,opt,name=source,proto3" json:"source,omitempty"`
	Symbol          string  `protobuf:"bytes,2,opt,name=symbol,proto3" json:"symbol,omitempty"`
	Price           float64 `protobuf:"fixed64,3,opt,name=price,proto3" json:"price,omitempty"`
	TimestampUnixMs int64   `protobuf:"varint,4,opt,name=timestamp_unix_ms,json=timestampUnixMs,proto3" json:"timestamp_unix_ms,omitempty"`
}

func (m *CryptoPriceTick) String() string { return fmt.Sprintf("%+v", *m) }

type TokenPriceTick struct {
	Source          string  `protobuf:"bytes,1,opt,name=source,proto3" json:"source,omitempty"`
	MarketId        string  `protobuf:"bytes,2,opt,name=market_id,json=marketId,proto3" json:"market_id,omitempty"`
	ConditionId     string  `protobuf:"bytes,3,opt,name=condition_id,json=conditionId,proto3" json:"condition_id,omitempty"`
	TokenId         string  `protobuf:"bytes,4,opt,name=token_id,json=tokenId,proto3" json:"token_id,omitempty"`
	Side            string  `protobuf:"bytes,5,opt,name=side,proto3" json:"side,omitempty"`
	Price           float64 `protobuf:"fixed64,6,opt,name=price,proto3" json:"price,omitempty"`
	TimestampUnixMs int64   `protobuf:"varint,7,opt,name=timestamp_unix_ms,json=timestampUnixMs,proto3" json:"timestamp_unix_ms,omitempty"`
}

func (m *TokenPriceTick) String() string { return fmt.Sprintf("%+v", *m) }

type MarketInfo struct {
	Source             string `protobuf:"bytes,1,opt,name=source,proto3" json:"source,omitempty"`
	MarketId           string `protobuf:"bytes,2,opt,name=market_id,json=marketId,proto3" json:"market_id,omitempty"`
	ConditionId        string `protobuf:"bytes,3,opt,name=condition_id,json=conditionId,proto3" json:"condition_id,omitempty"`
	CryptoSymbol       string `protobuf:"bytes,4,opt,name=crypto_symbol,json=cryptoSymbol,proto3" json:"crypto_symbol,omitempty"`
	TimeframeMinutes   int32  `protobuf:"varint,5,opt,name=timeframe_minutes,json=timeframeMinutes,proto3" json:"timeframe_minutes,omitempty"`
	UpTokenId          string `protobuf:"bytes,6,opt,name=up_token_id,json=upTokenId,proto3" json:"up_token_id,omitempty"`
	DownTokenId        string `protobuf:"bytes,7,opt,name=down_token_id,json=downTokenId,proto3" json:"down_token_id,omitempty"`
	StartUnixMs        int64  `protobuf:"varint,8,opt,name=start_unix_ms,json=startUnixMs,proto3" json:"start_unix_ms,omitempty"`
	EndUnixMs          int64  `protobuf:"varint,9,opt,name=end_unix_ms,json=endUnixMs,proto3" json:"end_unix_ms,omitempty"`
	DiscoveredAtUnixMs int64  `protobuf:"varint,10,opt,name=discovered_at_unix_ms,json=discoveredAtUnixMs,proto3" json:"discovered_at_unix_ms,omitempty"`
	Closed             bool   `protobuf:"varint,11,opt,name=closed,proto3" json:"closed,omitempty"`
}

func (m *MarketInfo) String() string { return fmt.Sprintf("%+v", *m) }
