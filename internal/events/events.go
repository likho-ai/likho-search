// Package events connects to the event bus (NATS JetStream) and takes the events this service
// listens to, as named in likho-contracts/streams.yaml.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Streams and subjects from streams.yaml.
const (
	Source                  = "likho-search"
	StreamEvents            = "LIKHO"
	StreamKeep              = "LIKHO_KEEP"
	CompletedSubject        = "likho.transcription.completed"
	CorrectedSubject        = "likho.transcript.corrected"
	RecordingDeletedSubject = "likho.recording.deleted"
)

// Event is a CloudEvents 1.0 envelope with the data still raw.
type Event struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Subject string          `json:"subject"`
	Data    json.RawMessage `json:"data"`
}

// Handler takes one event. Returning an error has the event delivered again later.
type Handler func(ctx context.Context, event Event) error

// Bus is the connection to the event bus.
type Bus struct {
	conn *nats.Conn
	js   jetstream.JetStream

	mu        sync.Mutex // guards the two lists: Take runs in the service's goroutine, Close and Forget in another
	consumers []jetstream.ConsumeContext
	durables  [][2]string // stream, consumer name
}

// Connect opens the connection and checks that the streams exist.
func Connect(ctx context.Context, url string) (*Bus, error) {
	// While NATS is not there yet (it may be starting at the same time) keep trying until ctx
	// ends; once connected, the client reconnects on its own.
	wait := time.Second
	for {
		bus, err := open(ctx, url)
		if err == nil {
			return bus, nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("the event bus at %s did not answer in time: %w", url, err)
		case <-timer.C:
		}
		wait = min(wait*2, 10*time.Second)
	}
}

func open(ctx context.Context, url string) (*Bus, error) {
	conn, err := nats.Connect(url, nats.Name(Source), nats.MaxReconnects(-1), nats.Timeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("event bus: %w", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("event bus: %w", err)
	}
	for _, stream := range []string{StreamEvents, StreamKeep} {
		if _, err := js.Stream(ctx, stream); err != nil {
			conn.Close()
			if errors.Is(err, jetstream.ErrStreamNotFound) {
				return nil, fmt.Errorf("stream %s does not exist; create the streams first (likho-infra: scripts/up.sh)", stream)
			}
			return nil, fmt.Errorf("event bus: %w", err)
		}
	}
	return &Bus{conn: conn, js: js}, nil
}

// Connected reports whether the bus can be reached right now.
func (b *Bus) Connected() bool { return b.conn.IsConnected() }

// Take starts a durable consumer named <group>-<last part of the subject> on the stream and
// hands every event on the subject to handle, one at a time, acknowledging the ones it took.
func (b *Bus) Take(ctx context.Context, stream, subject, group string, handle Handler, log *slog.Logger) error {
	name := group + "-" + subject[strings.LastIndex(subject, ".")+1:]
	consumer, err := b.js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       name,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("event bus: consumer %s: %w", name, err)
	}
	consuming, err := consumer.Consume(func(msg jetstream.Msg) {
		var event Event
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			log.Error("dropping a message that is not an event", "subject", subject, "error", err)
			_ = msg.Term()
			return
		}
		handleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := handle(handleCtx, event); err != nil {
			log.Warn("event not handled, will retry", "type", event.Type, "id", event.ID, "error", err)
			_ = msg.NakWithDelay(10 * time.Second)
			return
		}
		_ = msg.Ack()
	}, jetstream.PullMaxMessages(1))
	if err != nil {
		return fmt.Errorf("event bus: consume %s: %w", name, err)
	}
	b.mu.Lock()
	b.consumers = append(b.consumers, consuming)
	b.durables = append(b.durables, [2]string{stream, name})
	b.mu.Unlock()
	log.Info("taking " + subject + " as " + name)
	return nil
}

// Publish stores an event in the stream (tests publish what the other services would).
func (b *Bus) Publish(ctx context.Context, subject string, id string, event any) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = b.js.Publish(ctx, subject, body, jetstream.WithMsgID(id))
	return err
}

// Forget deletes the durable consumers this connection created (tests, which use names of
// their own; a real instance leaves its consumers for the next instance to continue from).
func (b *Bus) Forget(ctx context.Context) {
	b.mu.Lock()
	consumers, durables := b.consumers, b.durables
	b.consumers, b.durables = nil, nil
	b.mu.Unlock()
	for _, c := range consumers {
		c.Stop()
	}
	for _, d := range durables {
		_ = b.js.DeleteConsumer(ctx, d[0], d[1])
	}
}

// Close stops the consumers and closes the connection.
func (b *Bus) Close() {
	b.mu.Lock()
	consumers := b.consumers
	b.consumers = nil
	b.mu.Unlock()
	for _, c := range consumers {
		c.Stop()
	}
	b.conn.Close()
}
