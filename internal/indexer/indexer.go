// Package indexer keeps the index current: when a transcript is completed or corrected its lines
// are fetched from likho-transcription and stored; when a recording is deleted its lines go.
package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	transcriptionv1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/transcription/v1"
	"github.com/likho-ai/likho-contracts/packages/go/gen/likho/transcription/v1/transcriptionv1connect"

	"github.com/likho-ai/likho-search/internal/events"
	"github.com/likho-ai/likho-search/internal/index"
	"github.com/likho-ai/likho-search/internal/metrics"
)

// Transcripts is where transcripts come from (likho-transcription's gRPC service).
type Transcripts interface {
	GetTranscript(ctx context.Context, req *connect.Request[transcriptionv1.GetTranscriptRequest]) (*connect.Response[transcriptionv1.GetTranscriptResponse], error)
}

// NewTranscriptsClient connects to likho-transcription over gRPC: HTTP/2 without TLS, as the
// services talk inside the cluster.
func NewTranscriptsClient(addr string, timeout time.Duration) Transcripts {
	// HTTP/2 without TLS (h2c) over a plain TCP connection.
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Timeout: timeout, Transport: &http.Transport{Protocols: protocols}}
	return transcriptionv1connect.NewTranscriptionServiceClient(client, "http://"+addr, connect.WithGRPC())
}

// Indexer turns transcripts into indexed lines.
type Indexer struct {
	metrics     *metrics.Metrics
	index       *index.Index
	transcripts Transcripts
	log         *slog.Logger
}

// New returns an indexer.
func New(ix *index.Index, transcripts Transcripts, log *slog.Logger) *Indexer {
	return &Indexer{index: ix, transcripts: transcripts, log: log}
}

// Reindex fetches a transcript and replaces the recording's lines with it. It returns how
// many lines are now indexed.
// WithMetrics counts the transcripts and lines indexed.
func (in *Indexer) WithMetrics(m *metrics.Metrics) *Indexer {
	in.metrics = m
	return in
}

func (in *Indexer) Reindex(ctx context.Context, transcriptID, workspaceID string) (int, error) {
	n, err := in.reindex(ctx, transcriptID, workspaceID)
	if in.metrics != nil {
		if err != nil {
			in.metrics.Reindexes.Add(ctx, 1, metrics.Outcome("failed"))
		} else {
			in.metrics.Reindexes.Add(ctx, 1, metrics.Outcome("indexed"))
			in.metrics.SegmentsIndexed.Add(ctx, int64(n))
		}
	}
	return n, err
}

func (in *Indexer) reindex(ctx context.Context, transcriptID, workspaceID string) (int, error) {
	if transcriptID == "" || workspaceID == "" {
		return 0, errors.New("a transcript id and a workspace id are required")
	}
	response, err := in.transcripts.GetTranscript(ctx, connect.NewRequest(&transcriptionv1.GetTranscriptRequest{Id: transcriptID}))
	if err != nil {
		return 0, fmt.Errorf("transcript %s: %w", transcriptID, err)
	}
	transcript := response.Msg.GetTranscript()
	lines := Lines(transcript, workspaceID)
	if err := in.index.Replace(ctx, transcript.GetRecordingId(), lines); err != nil {
		return 0, err
	}
	in.log.Info("indexed", "transcript", transcriptID, "recording", transcript.GetRecordingId(), "lines", len(lines))
	return len(lines), nil
}

// Lines converts a transcript into index documents.
func Lines(transcript *transcriptionv1.Transcript, workspaceID string) []index.Line {
	created := time.Now().Unix()
	if transcript.GetCreatedAt() != nil {
		created = transcript.GetCreatedAt().AsTime().Unix()
	}
	lines := make([]index.Line, 0, len(transcript.GetSegments()))
	for _, segment := range transcript.GetSegments() {
		lines = append(lines, index.Line{
			ID:           fmt.Sprintf("%s-%d", transcript.GetId(), segment.GetIndex()),
			TranscriptID: transcript.GetId(),
			RecordingID:  transcript.GetRecordingId(),
			WorkspaceID:  workspaceID,
			Index:        segment.GetIndex(),
			StartSeconds: segment.GetStartSeconds(),
			EndSeconds:   segment.GetEndSeconds(),
			TextRoman:    segment.GetTextRoman(),
			TextScript:   segment.GetTextScript(),
			Language:     transcript.GetLanguage().GetDetected(),
			CreatedAt:    created,
		})
	}
	return lines
}

// Listen takes the three events from the bus.
func (in *Indexer) Listen(ctx context.Context, bus *events.Bus, group string) error {
	byTranscript := func(ctx context.Context, event events.Event) error {
		var data struct {
			TranscriptID string `json:"transcript_id"`
			WorkspaceID  string `json:"workspace_id"`
		}
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return fmt.Errorf("event %s: %w", event.ID, err)
		}
		_, err := in.Reindex(ctx, data.TranscriptID, data.WorkspaceID)
		return err
	}
	deleted := func(ctx context.Context, event events.Event) error {
		var data struct {
			RecordingID string `json:"recording_id"`
		}
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return fmt.Errorf("event %s: %w", event.ID, err)
		}
		if err := in.index.DeleteRecording(ctx, data.RecordingID); err != nil {
			return err
		}
		in.log.Info("forgot the lines of a deleted recording", "recording", data.RecordingID)
		return nil
	}
	if err := bus.Take(ctx, events.StreamEvents, events.CompletedSubject, group, byTranscript, in.log); err != nil {
		return err
	}
	if err := bus.Take(ctx, events.StreamKeep, events.CorrectedSubject, group, byTranscript, in.log); err != nil {
		return err
	}
	return bus.Take(ctx, events.StreamEvents, events.RecordingDeletedSubject, group, deleted, in.log)
}
