package app_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	searchv1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/search/v1"
	transcriptionv1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/transcription/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/likho-ai/likho-search/internal/events"
	"github.com/likho-ai/likho-search/internal/testenv"
)

func TestARecordingsFactsNarrowTheSearch(t *testing.T) {
	transcripts := &fakeTranscripts{transcripts: map[string]*transcriptionv1.Transcript{}}
	r := start(t, transcripts)
	ctx := context.Background()
	workspace := "wsp_" + strings.ToUpper(testenv.Unique())
	sale := "rec_" + strings.ToUpper(testenv.Unique())
	support := "rec_" + strings.ToUpper(testenv.Unique())
	publish := func(subject, eventType string, data map[string]any) {
		t.Helper()
		id := "evt_" + testenv.Unique()
		err := r.bus.Publish(ctx, subject, id, map[string]any{
			"specversion": "1.0", "id": id, "source": "test", "type": eventType, "time": time.Now().UTC().Format(time.RFC3339),
			"subject": data["recording_id"], "datacontenttype": "application/json", "data": data,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// likho-api says what it knows about a recording as soon as it exists, before any transcript.
	callTime := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	publish(events.RecordingUpdatedSubject, "likho.recording.updated.v1", map[string]any{
		"recording_id": sale, "workspace_id": workspace, "source": "ameyo", "external_id": "d000-0001", "name": "d000-0001.mp3",
		"call_time": callTime.Format(time.RFC3339), "attributes": map[string]string{"campaign": "sale", "agent": "agent-x", "disposition": "sold"},
	})
	transcripts.transcripts["trn_sale"] = transcript("trn_sale", sale, 1, [2]string{"refund de dijiye please", "रिफंड दे दीजिए प्लीज"})
	transcripts.transcripts["trn_support"] = transcript("trn_support", support, 1, [2]string{"refund kab milega", "रिफंड कब मिलेगा"})
	for id, recording := range map[string]string{"trn_sale": sale, "trn_support": support} {
		publish(events.CompletedSubject, "likho.transcription.completed.v1", map[string]any{
			"job_id": "job_" + id, "recording_id": recording, "transcript_id": id, "workspace_id": workspace, "version": 1,
		})
	}
	waitFor(t, r.client, workspace, "refund", 2)
	// The other recording's facts come after its lines.
	publish(events.RecordingUpdatedSubject, "likho.recording.updated.v1", map[string]any{
		"recording_id": support, "workspace_id": workspace, "source": "upload", "attributes": map[string]string{"campaign": "support", "agent": "agent-x"},
	})

	search := func(req *searchv1.SearchRequest) []string {
		t.Helper()
		req.WorkspaceId, req.Query = workspace, "refund"
		deadline := time.Now().Add(20 * time.Second)
		for {
			response, err := r.client.Search(ctx, connect.NewRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0)
			for _, hit := range response.Msg.GetHits() {
				ids = append(ids, hit.GetRecordingId())
			}
			if len(ids) > 0 || time.Now().After(deadline) {
				return ids
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	if got := search(&searchv1.SearchRequest{Campaign: "sale", Agent: "agent-x"}); len(got) != 1 || got[0] != sale {
		t.Fatalf("sale calls of agent-x: %v", got)
	}
	if got := search(&searchv1.SearchRequest{Campaign: "support"}); len(got) != 1 || got[0] != support {
		t.Fatalf("support calls (facts after the lines): %v", got)
	}
	if got := search(&searchv1.SearchRequest{Agent: "agent-x", CallSince: timestamppb.New(callTime.Add(-time.Minute))}); len(got) != 1 || got[0] != sale {
		t.Fatalf("agent-x since the call: %v", got)
	}
	response, err := r.client.Search(ctx, connect.NewRequest(&searchv1.SearchRequest{WorkspaceId: workspace, Query: "refund", Campaign: "sale"}))
	if err != nil {
		t.Fatal(err)
	}
	hit := response.Msg.GetHits()[0]
	if hit.GetCampaign() != "sale" || hit.GetAgent() != "agent-x" || hit.GetSource() != "ameyo" || !hit.GetCallTime().AsTime().Equal(callTime) {
		t.Fatalf("the facts on the hit: %v", hit)
	}
}
