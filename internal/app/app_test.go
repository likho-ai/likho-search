package app_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	commonv1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/common/v1"
	searchv1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/search/v1"
	"github.com/likho-ai/likho-contracts/packages/go/gen/likho/search/v1/searchv1connect"
	transcriptionv1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/transcription/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/likho-ai/likho-search/internal/app"
	"github.com/likho-ai/likho-search/internal/events"
	"github.com/likho-ai/likho-search/internal/index"
	"github.com/likho-ai/likho-search/internal/testenv"
)

// fakeTranscripts stands in for likho-transcription: transcripts by id, made up text.
type fakeTranscripts struct {
	mu          sync.Mutex
	transcripts map[string]*transcriptionv1.Transcript
}

func (f *fakeTranscripts) GetTranscript(_ context.Context, req *connect.Request[transcriptionv1.GetTranscriptRequest]) (*connect.Response[transcriptionv1.GetTranscriptResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	transcript, ok := f.transcripts[req.Msg.GetId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("no such transcript"))
	}
	return connect.NewResponse(&transcriptionv1.GetTranscriptResponse{Transcript: transcript}), nil
}

func transcript(id, recording string, version uint32, texts ...[2]string) *transcriptionv1.Transcript {
	t := &transcriptionv1.Transcript{
		Id: id, RecordingId: recording, JobId: "job_" + id, Version: version,
		Language:  &commonv1.LanguageDetection{Detected: "hi", Probability: 0.9, DecodedAs: "hi", Policy: "auto"},
		CreatedAt: timestamppb.Now(),
	}
	for i, text := range texts {
		t.Segments = append(t.Segments, &commonv1.Segment{
			Index: uint32(i), StartSeconds: float64(i) * 5, EndSeconds: float64(i)*5 + 4, TextRoman: text[0], TextScript: text[1],
		})
	}
	return t
}

type running struct {
	app    *app.App
	client searchv1connect.SearchServiceClient
	bus    *events.Bus
}

func start(t *testing.T, transcripts *fakeTranscripts) running {
	t.Helper()
	cfg := testenv.Config(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	service, err := app.New(ctx, cfg, log, app.Options{Transcripts: transcripts})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(runCtx) }()
	t.Cleanup(func() {
		service.Bus().Forget(context.Background())
		stop()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the service did not stop in time")
		}
		ix, err := index.Open(context.Background(), cfg.MeiliURL, cfg.MeiliAPIKey, cfg.IndexName, cfg.MaxHits)
		if err == nil {
			_ = ix.Drop(context.Background())
		}
	})
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	httpClient := &http.Client{Transport: &http.Transport{Protocols: protocols}}
	client := searchv1connect.NewSearchServiceClient(httpClient, "http://"+service.GRPCAddr(), connect.WithGRPC())
	return running{app: service, client: client, bus: service.Bus()}
}

// waitFor polls until the search returns the expected total or the deadline passes.
func waitFor(t *testing.T, client searchv1connect.SearchServiceClient, workspace, query string, total uint32) *searchv1.SearchResponse {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		response, err := client.Search(context.Background(), connect.NewRequest(&searchv1.SearchRequest{WorkspaceId: workspace, Query: query}))
		if err == nil && response.Msg.GetTotal() == total {
			return response.Msg
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			t.Fatalf("expected %d hits for %q, got %d", total, query, response.Msg.GetTotal())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestEventsKeepTheIndexCurrent(t *testing.T) {
	transcripts := &fakeTranscripts{transcripts: map[string]*transcriptionv1.Transcript{}}
	r := start(t, transcripts)
	ctx := context.Background()
	workspace := "wsp_" + strings.ToUpper(testenv.Unique())
	recording := "rec_" + strings.ToUpper(testenv.Unique())

	// A completed transcription: its lines become searchable.
	transcripts.transcripts["trn_v1"] = transcript("trn_v1", recording, 1,
		[2]string{"namaskar ji, order confirm hai", "नमस्कार जी, ऑर्डर कन्फर्म है"})
	publish := func(subject, eventType, id string, data map[string]any) {
		t.Helper()
		err := r.bus.Publish(ctx, subject, id, map[string]any{
			"specversion": "1.0", "id": id, "source": "test", "type": eventType, "time": time.Now().UTC().Format(time.RFC3339),
			"subject": recording, "datacontenttype": "application/json", "data": data,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	publish(events.CompletedSubject, "likho.transcription.completed.v1", "evt_"+testenv.Unique(), map[string]any{
		"job_id": "job_1", "recording_id": recording, "transcript_id": "trn_v1", "workspace_id": workspace, "version": 1,
	})
	response := waitFor(t, r.client, workspace, "namaskar", 1)
	hit := response.GetHits()[0]
	if hit.GetRecordingId() != recording || hit.GetTranscriptId() != "trn_v1" || !strings.Contains(hit.GetHighlightRoman(), "<mark>") {
		t.Fatalf("unexpected hit: %v", hit)
	}

	// A correction: the newer transcript replaces the lines (the same recording).
	transcripts.transcripts["trn_v2"] = transcript("trn_v2", recording, 2,
		[2]string{"namaskar ji, order confirm hai", "नमस्कार जी, ऑर्डर कन्फर्म है"},
		[2]string{"delivery kal hogi", "डिलीवरी कल होगी"})
	publish(events.CorrectedSubject, "likho.transcript.corrected.v1", "evt_"+testenv.Unique(), map[string]any{
		"transcript_id": "trn_v2", "recording_id": recording, "workspace_id": workspace, "user_id": "usr_1",
		"segment_index": 1, "layer": "roman", "before": "x", "after": "delivery kal hogi",
	})
	response = waitFor(t, r.client, workspace, "delivery", 1)
	if response.GetHits()[0].GetTranscriptId() != "trn_v2" {
		t.Fatalf("expected the corrected transcript, got %v", response.GetHits()[0])
	}
	if response = waitFor(t, r.client, workspace, "namaskar", 1); response.GetHits()[0].GetTranscriptId() != "trn_v2" {
		t.Fatalf("the older transcript's line survived: %v", response.GetHits()[0])
	}

	// Reindex by hand reports the count; a deleted recording leaves no line behind.
	reindexed, err := r.client.Reindex(ctx, connect.NewRequest(&searchv1.ReindexRequest{TranscriptId: "trn_v2", WorkspaceId: workspace}))
	if err != nil || reindexed.Msg.GetLines() != 2 {
		t.Fatalf("reindex: %v, %v", reindexed, err)
	}
	publish(events.RecordingDeletedSubject, "likho.recording.deleted.v1", "evt_"+testenv.Unique(), map[string]any{
		"recording_id": recording, "workspace_id": workspace,
	})
	waitFor(t, r.client, workspace, "namaskar", 0)

	// Refusals.
	if _, err := r.client.Search(ctx, connect.NewRequest(&searchv1.SearchRequest{Query: "x"})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a search without a workspace: %v", err)
	}
	if _, err := r.client.Reindex(ctx, connect.NewRequest(&searchv1.ReindexRequest{TranscriptId: "trn_missing", WorkspaceId: workspace})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("reindexing a missing transcript: %v", err)
	}

	// Health.
	resp, err := http.Get("http://" + r.app.HTTPAddr() + "/readyz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz: %v %v", resp, err)
	}
	_ = resp.Body.Close()

	// Metrics: Prometheus text with the calls answered above.
	resp, err = http.Get("http://" + r.app.HTTPAddr() + "/metrics")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics: %v %v", resp, err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), "likho_search_requests_total") || !strings.Contains(string(body), "likho_search_segments_indexed_total") {
		t.Fatalf("metrics: %s", body)
	}
}
