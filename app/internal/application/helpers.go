package application

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/domain"
)

type tokenBinding struct {
	marketID string
	side     string
}

func collectSortedTokenIDs(markets map[string]domain.ActiveMarket) []string {
	unique := map[string]struct{}{}
	for _, market := range markets {
		if token := strings.TrimSpace(market.UpTokenID); token != "" {
			unique[token] = struct{}{}
		}
		if token := strings.TrimSpace(market.DownTokenID); token != "" {
			unique[token] = struct{}{}
		}
	}

	out := make([]string, 0, len(unique))
	for tokenID := range unique {
		out = append(out, tokenID)
	}
	sort.Strings(out)
	return out
}

func bindTokens(markets map[string]domain.ActiveMarket) map[string]tokenBinding {
	out := make(map[string]tokenBinding, len(markets)*2)
	for _, market := range markets {
		if token := strings.TrimSpace(market.UpTokenID); token != "" {
			out[token] = tokenBinding{marketID: market.MarketID, side: "UP"}
		}
		if token := strings.TrimSpace(market.DownTokenID); token != "" {
			out[token] = tokenBinding{marketID: market.MarketID, side: "DOWN"}
		}
	}
	return out
}

func marketWindow(now time.Time, marketType domain.MarketType) (time.Time, time.Time) {
	if marketType == domain.MarketTypeSixtyMinutes {
		ny := mustLoadLocationOrUTC("America/New_York")
		inNY := now.In(ny).Truncate(time.Hour)
		start := time.Date(inNY.Year(), inNY.Month(), inNY.Day(), inNY.Hour(), 0, 0, 0, ny)
		end := start.Add(time.Hour)
		return start.UTC(), end.UTC()
	}

	minutes := marketType.Minutes()
	truncated := now.UTC().Truncate(time.Duration(minutes) * time.Minute)
	return truncated, truncated.Add(time.Duration(minutes) * time.Minute)
}

func buildMarketSlug(crypto domain.Crypto, marketType domain.MarketType, start time.Time) string {
	if marketType == domain.MarketTypeSixtyMinutes {
		ny := mustLoadLocationOrUTC("America/New_York")
		inNY := start.In(ny)
		month := strings.ToLower(inNY.Month().String())
		day := inNY.Day()
		hour12 := inNY.Format("3")
		ampm := strings.ToLower(inNY.Format("PM"))

		return fmt.Sprintf(
			"%s-up-or-down-%s-%d-%s%s-et",
			strings.ToLower(crypto.FullName),
			month,
			day,
			hour12,
			ampm,
		)
	}

	return fmt.Sprintf(
		"%s-updown-%dm-%d",
		strings.ToLower(crypto.MinName),
		marketType.Minutes(),
		start.UTC().Unix(),
	)
}

func sanitizeSubjectToken(input string) string {
	value := strings.ToLower(strings.TrimSpace(input))
	value = strings.ReplaceAll(value, ".", "_")
	value = strings.ReplaceAll(value, "*", "_")
	value = strings.ReplaceAll(value, ">", "_")
	value = strings.ReplaceAll(value, " ", "_")
	return value
}

func mustLoadLocationOrUTC(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil || loc == nil {
		return time.UTC
	}
	return loc
}
