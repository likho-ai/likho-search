// Package config reads the service's settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is everything that can be set from outside. The defaults match the likho-infra local stack.
type Config struct {
	Env      string
	LogLevel string

	HTTPPort int // /healthz, /readyz
	GRPCPort int // likho.search.v1.SearchService

	NATSURL string
	// NATSConnectTimeout is how long the start keeps trying to reach NATS before giving up.
	NATSConnectTimeout time.Duration
	// OTLPEndpoint is where metrics are pushed as well (OTLP/HTTP); empty = only GET /metrics.
	OTLPEndpoint string

	MeiliURL    string
	MeiliAPIKey string
	// IndexName is the Meilisearch index that holds every line ("segments").
	IndexName string

	// TranscriptionGRPCAddr is where transcripts are fetched from when an event says one is ready.
	TranscriptionGRPCAddr string
	RPCTimeout            time.Duration

	// ConsumerGroup prefixes the names of the durable consumers (likho-search-<event>).
	ConsumerGroup string
	// ConsumersEnabled is false for an instance that only answers searches.
	ConsumersEnabled bool
	// MaxHits is the most lines one search may page through (Meilisearch's maxTotalHits).
	MaxHits int
}

// Load reads the configuration and checks it.
func Load() (Config, error) {
	var problems []string
	number := func(name string, fallback int) int {
		raw, ok := os.LookupEnv(name)
		if !ok || raw == "" {
			return fallback
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			problems = append(problems, fmt.Sprintf("%s must be a whole number, got %q", name, raw))
			return fallback
		}
		return value
	}
	text := func(name, fallback string) string {
		if value, ok := os.LookupEnv(name); ok && value != "" {
			return value
		}
		return fallback
	}
	flag := func(name string, fallback bool) bool {
		raw, ok := os.LookupEnv(name)
		if !ok || raw == "" {
			return fallback
		}
		switch strings.ToLower(raw) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
		problems = append(problems, fmt.Sprintf("%s must be true or false, got %q", name, raw))
		return fallback
	}

	cfg := Config{
		Env:                   text("LIKHO_ENV", "development"),
		LogLevel:              text("LOG_LEVEL", "INFO"),
		HTTPPort:              number("HTTP_PORT", 4040),
		GRPCPort:              number("GRPC_PORT", 5040),
		NATSURL:               text("NATS_URL", "nats://localhost:4222"),
		NATSConnectTimeout:    time.Duration(number("NATS_CONNECT_TIMEOUT_SECONDS", 120)) * time.Second,
		OTLPEndpoint:          text("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		MeiliURL:              strings.TrimRight(text("MEILI_URL", "http://localhost:7700"), "/"),
		MeiliAPIKey:           text("MEILI_API_KEY", "likho-dev-master-key"),
		IndexName:             text("INDEX_NAME", "segments"),
		TranscriptionGRPCAddr: text("TRANSCRIPTION_GRPC_ADDR", "localhost:5020"),
		RPCTimeout:            time.Duration(number("RPC_TIMEOUT_SECONDS", 30)) * time.Second,
		ConsumerGroup:         text("CONSUMER_GROUP", "likho-search"),
		ConsumersEnabled:      flag("CONSUMERS_ENABLED", true),
		MaxHits:               number("MAX_HITS", 10000),
	}
	if (cfg.Env == "staging" || cfg.Env == "production") && cfg.MeiliAPIKey == "likho-dev-master-key" {
		problems = append(problems, "MEILI_API_KEY must be set in "+cfg.Env)
	}
	if cfg.MaxHits < 100 {
		problems = append(problems, "MAX_HITS must be at least 100")
	}
	if len(problems) > 0 {
		return Config{}, errors.New(strings.Join(problems, "; "))
	}
	return cfg, nil
}
