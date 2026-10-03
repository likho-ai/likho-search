// Package testenv helps tests that need the likho-infra stack (Meilisearch and NATS).
//
// Start the stack first:  likho-infra> bash scripts/up.sh   (or .\stack.ps1 up)
// Without it these tests are skipped locally; with LIKHO_REQUIRE_STACK=1 (set in CI) they fail instead.
package testenv

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/likho-ai/likho-search/internal/config"
)

// Config returns the service's configuration for a test: the local stack, an index of its own
// (dropped by the test), free ports, and a consumer group of its own.
func Config(t *testing.T) config.Config {
	t.Helper()
	t.Setenv("LIKHO_ENV", "test")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	requireStack(t, cfg)
	suffix := Unique()
	cfg.HTTPPort, cfg.GRPCPort = 0, 0
	cfg.IndexName = "test_" + suffix
	cfg.ConsumerGroup = "likho-search-test-" + suffix
	return cfg
}

// Unique returns a short random id for names that must not collide between test runs.
func Unique() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func requireStack(t *testing.T, cfg config.Config) {
	t.Helper()
	var missing []string
	for name, address := range map[string]string{"Meilisearch": hostOf(cfg.MeiliURL), "NATS": hostOf(cfg.NATSURL)} {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			missing = append(missing, name)
			continue
		}
		_ = conn.Close()
	}
	if len(missing) == 0 {
		return
	}
	message := strings.Join(missing, ", ") + " not available; start the likho-infra stack"
	if os.Getenv("LIKHO_REQUIRE_STACK") == "1" {
		t.Fatal(message)
	}
	t.Skip(message)
}

func hostOf(address string) string {
	parsed, err := url.Parse(address)
	if err != nil {
		return address
	}
	return parsed.Host
}
