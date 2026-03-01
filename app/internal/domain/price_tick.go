package domain

import "time"

type InstrumentType string

const (
	InstrumentTypeCrypto   InstrumentType = "CRYPTO"
	InstrumentTypeBetToken InstrumentType = "BET_TOKEN"
)

type PriceTick struct {
	Source         string
	InstrumentType InstrumentType
	Symbol         string
	MarketID       string
	TokenID        string
	Side           string
	Price          float64
	Timestamp      time.Time
}

type MarketTokens struct {
	MarketID    string
	UpTokenID   string
	DownTokenID string
}

type Crypto struct {
	MinName   string
	FullName  string
	ConvertTo string
}

type MarketType int

const (
	MarketTypeFiveMinutes    MarketType = 5
	MarketTypeFifteenMinutes MarketType = 15
	MarketTypeSixtyMinutes   MarketType = 60
)

func (m MarketType) Minutes() int {
	return int(m)
}

type ActiveMarket struct {
	MarketID         string
	ConditionID      string
	Crypto           Crypto
	TimeframeMinutes int
	UpTokenID        string
	DownTokenID      string
	StartTime        time.Time
	EndTime          time.Time
	Closed           bool
}

type TokenPriceUpdate struct {
	TokenID    string
	Price      float64
	Timestamp  time.Time
	RawPayload string
}
