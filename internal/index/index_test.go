package index_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/likho-ai/likho-search/internal/index"
	"github.com/likho-ai/likho-search/internal/testenv"
)

// Lines of two recordings in one workspace and one in another. The text is made up.
func lines(workspace, recording, transcript string, created int64, texts ...[2]string) []index.Line {
	out := make([]index.Line, 0, len(texts))
	for i, t := range texts {
		out = append(out, index.Line{
			ID: transcript + "-" + string(rune('0'+i)), TranscriptID: transcript, RecordingID: recording, WorkspaceID: workspace,
			Index: uint32(i), StartSeconds: float64(i) * 5, EndSeconds: float64(i)*5 + 4,
			TextRoman: t[0], TextScript: t[1], Language: "hi", CreatedAt: created,
		})
	}
	return out
}

func open(t *testing.T) (*index.Index, context.Context) {
	t.Helper()
	cfg := testenv.Config(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	ix, err := index.Open(ctx, cfg.MeiliURL, cfg.MeiliAPIKey, cfg.IndexName, cfg.MaxHits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Drop(context.Background()) })
	return ix, ctx
}

func TestSearchFindsLinesInEitherLayerWithTypos(t *testing.T) {
	ix, ctx := open(t)
	now := time.Now().Unix()
	a := lines("wsp_A", "rec_1", "trn_1", now,
		[2]string{"namaskar ji aapka order ready hai", "नमस्कार जी आपका ऑर्डर रेडी है"},
		[2]string{"delivery kal tak ho jayegi", "डिलीवरी कल तक हो जाएगी"},
		[2]string{"haan ji bilkul theek hai", "हाँ जी बिल्कुल ठीक है"})
	b := lines("wsp_A", "rec_2", "trn_2", now-86400,
		[2]string{"appointment confirm kar dijiye", "अपॉइंटमेंट कन्फर्म कर दीजिए"})
	other := lines("wsp_B", "rec_3", "trn_3", now,
		[2]string{"namaskar, kaise hain aap", "नमस्कार, कैसे हैं आप"})
	for _, set := range [][]index.Line{a, b, other} {
		if err := ix.Replace(ctx, set[0].RecordingID, set); err != nil {
			t.Fatal(err)
		}
	}

	// Roman query, one typo, only this workspace.
	result, err := ix.Search(ctx, index.Query{WorkspaceID: "wsp_A", Text: "namaskaar"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || len(result.Hits) != 1 || result.Hits[0].RecordingID != "rec_1" {
		t.Fatalf("expected the one line of rec_1, got %+v", result)
	}
	if !strings.Contains(result.Hits[0].HighlightRoman, "<mark>namaskar</mark>") {
		t.Fatalf("match not marked: %q", result.Hits[0].HighlightRoman)
	}

	// Devanagari query finds the same line through the script layer.
	result, err = ix.Search(ctx, index.Query{WorkspaceID: "wsp_A", Text: "नमस्कार"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || !strings.Contains(result.Hits[0].HighlightScript, "<mark>नमस्कार</mark>") {
		t.Fatalf("script layer not searched: %+v", result)
	}

	// A time window and a recording filter narrow it down.
	result, err = ix.Search(ctx, index.Query{WorkspaceID: "wsp_A", Text: "kar", Since: time.Unix(now-3600, 0)})
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range result.Hits {
		if hit.RecordingID == "rec_2" {
			t.Fatalf("yesterday's line returned inside today's window: %+v", result)
		}
	}
	result, err = ix.Search(ctx, index.Query{WorkspaceID: "wsp_A", Text: "kar", RecordingID: "rec_2"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || result.Hits[0].RecordingID != "rec_2" {
		t.Fatalf("recording filter: %+v", result)
	}

	// Paging reports the whole count.
	result, err = ix.Search(ctx, index.Query{WorkspaceID: "wsp_A", Text: "hai", PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.PageSize != 1 || len(result.Hits) != 1 || result.Total != 2 {
		t.Fatalf("paging: %+v", result)
	}
}

func TestReplaceAndDeleteKeepOneTranscriptPerRecording(t *testing.T) {
	ix, ctx := open(t)
	now := time.Now().Unix()
	first := lines("wsp_A", "rec_1", "trn_1", now, [2]string{"pehla version", "पहला वर्शन"})
	second := lines("wsp_A", "rec_1", "trn_2", now, [2]string{"doosra version", "दूसरा वर्शन"})
	if err := ix.Replace(ctx, "rec_1", first); err != nil {
		t.Fatal(err)
	}
	if err := ix.Replace(ctx, "rec_1", second); err != nil {
		t.Fatal(err)
	}
	result, err := ix.Search(ctx, index.Query{WorkspaceID: "wsp_A", Text: "version"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || result.Hits[0].TranscriptID != "trn_2" {
		t.Fatalf("expected only the newer transcript's line, got %+v", result)
	}
	if workspace, err := ix.Workspace(ctx, "rec_1"); err != nil || workspace != "wsp_A" {
		t.Fatalf("workspace of rec_1: %q, %v", workspace, err)
	}

	if err := ix.DeleteRecording(ctx, "rec_1"); err != nil {
		t.Fatal(err)
	}
	result, err = ix.Search(ctx, index.Query{WorkspaceID: "wsp_A", Text: "version"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 0 {
		t.Fatalf("lines survive the delete: %+v", result)
	}
	if workspace, _ := ix.Workspace(ctx, "rec_1"); workspace != "" {
		t.Fatalf("workspace after delete: %q", workspace)
	}
}

func TestSearchNeedsAWorkspace(t *testing.T) {
	ix, ctx := open(t)
	if _, err := ix.Search(ctx, index.Query{Text: "x"}); err == nil {
		t.Fatal("a search without a workspace must be refused")
	}
}
