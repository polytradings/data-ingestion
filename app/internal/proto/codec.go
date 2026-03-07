package proto

import (
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

func MarshalCryptoPriceTick(m *CryptoPriceTick) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	var b []byte
	if m.Source != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, m.Source)
	}
	if m.Symbol != "" {
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendString(b, m.Symbol)
	}
	if m.Price != 0 {
		b = protowire.AppendTag(b, 3, protowire.Fixed64Type)
		b = protowire.AppendFixed64(b, math.Float64bits(m.Price))
	}
	if m.TimestampUnixMs != 0 {
		b = protowire.AppendTag(b, 4, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.TimestampUnixMs))
	}
	return b, nil
}

func UnmarshalCryptoPriceTick(data []byte, m *CryptoPriceTick) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		switch num {
		case 1:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Source = v
			data = data[n:]
		case 2:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Symbol = v
			data = data[n:]
		case 3:
			v, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Price = math.Float64frombits(v)
			data = data[n:]
		case 4:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.TimestampUnixMs = int64(v)
			data = data[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
		}
	}
	return nil
}

func MarshalTokenPriceTick(m *TokenPriceTick) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	var b []byte
	if m.Source != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, m.Source)
	}
	if m.MarketId != "" {
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendString(b, m.MarketId)
	}
	if m.ConditionId != "" {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendString(b, m.ConditionId)
	}
	if m.TokenId != "" {
		b = protowire.AppendTag(b, 4, protowire.BytesType)
		b = protowire.AppendString(b, m.TokenId)
	}
	if m.Side != "" {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendString(b, m.Side)
	}
	if m.Price != 0 {
		b = protowire.AppendTag(b, 6, protowire.Fixed64Type)
		b = protowire.AppendFixed64(b, math.Float64bits(m.Price))
	}
	if m.TimestampUnixMs != 0 {
		b = protowire.AppendTag(b, 7, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.TimestampUnixMs))
	}
	return b, nil
}

func UnmarshalTokenPriceTick(data []byte, m *TokenPriceTick) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		switch num {
		case 1:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Source = v
			data = data[n:]
		case 2:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.MarketId = v
			data = data[n:]
		case 3:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.ConditionId = v
			data = data[n:]
		case 4:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.TokenId = v
			data = data[n:]
		case 5:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Side = v
			data = data[n:]
		case 6:
			v, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Price = math.Float64frombits(v)
			data = data[n:]
		case 7:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.TimestampUnixMs = int64(v)
			data = data[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
		}
	}
	return nil
}

func MarshalMarketInfo(m *MarketInfo) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	var b []byte
	if m.Source != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, m.Source)
	}
	if m.MarketId != "" {
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendString(b, m.MarketId)
	}
	if m.ConditionId != "" {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendString(b, m.ConditionId)
	}
	if m.CryptoSymbol != "" {
		b = protowire.AppendTag(b, 4, protowire.BytesType)
		b = protowire.AppendString(b, m.CryptoSymbol)
	}
	if m.TimeframeMinutes != 0 {
		b = protowire.AppendTag(b, 5, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.TimeframeMinutes))
	}
	if m.UpTokenId != "" {
		b = protowire.AppendTag(b, 6, protowire.BytesType)
		b = protowire.AppendString(b, m.UpTokenId)
	}
	if m.DownTokenId != "" {
		b = protowire.AppendTag(b, 7, protowire.BytesType)
		b = protowire.AppendString(b, m.DownTokenId)
	}
	if m.StartUnixMs != 0 {
		b = protowire.AppendTag(b, 8, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.StartUnixMs))
	}
	if m.EndUnixMs != 0 {
		b = protowire.AppendTag(b, 9, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.EndUnixMs))
	}
	if m.DiscoveredAtUnixMs != 0 {
		b = protowire.AppendTag(b, 10, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.DiscoveredAtUnixMs))
	}
	if m.Closed {
		b = protowire.AppendTag(b, 11, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	return b, nil
}

func UnmarshalMarketInfo(data []byte, m *MarketInfo) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		switch num {
		case 1:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Source = v
			data = data[n:]
		case 2:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.MarketId = v
			data = data[n:]
		case 3:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.ConditionId = v
			data = data[n:]
		case 4:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.CryptoSymbol = v
			data = data[n:]
		case 5:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.TimeframeMinutes = int32(v)
			data = data[n:]
		case 6:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.UpTokenId = v
			data = data[n:]
		case 7:
			v, n := protowire.ConsumeString(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.DownTokenId = v
			data = data[n:]
		case 8:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.StartUnixMs = int64(v)
			data = data[n:]
		case 9:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.EndUnixMs = int64(v)
			data = data[n:]
		case 10:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.DiscoveredAtUnixMs = int64(v)
			data = data[n:]
		case 11:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			m.Closed = v > 0
			data = data[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
		}
	}
	return nil
}
