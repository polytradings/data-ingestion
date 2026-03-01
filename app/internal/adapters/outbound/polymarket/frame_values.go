package polymarket

import (
	"fmt"
	"strconv"
	"strings"
)

func valueAsString(body map[string]any, keys ...string) (string, bool) {
	for _, key := range keys {
		value, ok := body[key]
		if !ok {
			continue
		}
		s := strings.TrimSpace(fmt.Sprintf("%v", value))
		if s != "" {
			return s, true
		}
	}
	return "", false
}

func valueAsFloat64(body map[string]any, keys ...string) (float64, bool) {
	for _, key := range keys {
		value, ok := body[key]
		if !ok {
			continue
		}
		s := strings.TrimSpace(fmt.Sprintf("%v", value))
		if s == "" {
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func valueAsInt64(body map[string]any, keys ...string) (int64, bool) {
	f, ok := valueAsFloat64(body, keys...)
	if !ok {
		return 0, false
	}
	return int64(f), true
}
