package index_test

import (
	"testing"
	"time"

	"github.com/likho-ai/likho-search/internal/index"
)

func TestFactsNarrowASearchWhicheverArrivesFirst(t *testing.T) {
	ix, ctx := open(t)
	now := time.Now().Unix()
	yesterday := now - 86400

	// The facts of rec_1 are known before its lines come; those of rec_2 arrive after.
	sale := index.Facts{RecordingID: "rec_1", WorkspaceID: "wsp_A", Source: "ameyo", Campaign: "sale", Agent: "agent-x", Disposition: "sold", CallTime: now}
	if err := ix.PutFacts(ctx, sale); err != nil {
		t.Fatal(err)
	}
	one := lines("wsp_A", "rec_1", "trn_1", now, [2]string{"refund de dijiye please", "रिफंड दे दीजिए प्लीज"})
	two := lines("wsp_A", "rec_2", "trn_2", now, [2]string{"refund kab milega", "रिफंड कब मिलेगा"})
	three := lines("wsp_A", "rec_3", "trn_3", now, [2]string{"refund nahi chahiye", "रिफंड नहीं चाहिए"})
	for _, set := range [][]index.Line{one, two, three} {
		if err := ix.Replace(ctx, set[0].RecordingID, set); err != nil {
			t.Fatal(err)
		}
	}
	support := index.Facts{RecordingID: "rec_2", WorkspaceID: "wsp_A", Source: "ameyo", Campaign: "support", Agent: "agent-x", Disposition: "resolved", CallTime: yesterday}
	if err := ix.PutFacts(ctx, support); err != nil {
		t.Fatal(err)
	}

	count := func(q index.Query) []string {
		t.Helper()
		q.WorkspaceID, q.Text = "wsp_A", "refund"
		result, err := ix.Search(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(result.Hits))
		for _, hit := range result.Hits {
			ids = append(ids, hit.RecordingID)
		}
		return ids
	}
	same := func(got []string, want ...string) bool {
		if len(got) != len(want) {
			return false
		}
		seen := map[string]bool{}
		for _, id := range got {
			seen[id] = true
		}
		for _, id := range want {
			if !seen[id] {
				return false
			}
		}
		return true
	}

	if got := count(index.Query{}); !same(got, "rec_1", "rec_2", "rec_3") {
		t.Fatalf("no filter: %v", got)
	}
	// "every sale call of agent-x with the word refund"
	if got := count(index.Query{Campaign: "sale", Agent: "agent-x"}); !same(got, "rec_1") {
		t.Fatalf("campaign and agent: %v", got)
	}
	// The facts that came after the lines count too.
	if got := count(index.Query{Agent: "agent-x"}); !same(got, "rec_1", "rec_2") {
		t.Fatalf("agent across both: %v", got)
	}
	if got := count(index.Query{Disposition: "resolved"}); !same(got, "rec_2") {
		t.Fatalf("disposition: %v", got)
	}
	if got := count(index.Query{Source: "ameyo"}); !same(got, "rec_1", "rec_2") {
		t.Fatalf("source (rec_3 has no facts): %v", got)
	}
	// A window on when the call happened: only today's.
	if got := count(index.Query{CallSince: time.Unix(now-3600, 0)}); !same(got, "rec_1") {
		t.Fatalf("call window: %v", got)
	}
	// Changed facts replace the old ones on every line.
	sale.Agent = "agent-y"
	if err := ix.PutFacts(ctx, sale); err != nil {
		t.Fatal(err)
	}
	if got := count(index.Query{Agent: "agent-x"}); !same(got, "rec_2") {
		t.Fatalf("after the change: %v", got)
	}
	// A new transcript of rec_1 keeps the facts.
	if err := ix.Replace(ctx, "rec_1", lines("wsp_A", "rec_1", "trn_9", now, [2]string{"refund ho gaya", "रिफंड हो गया"})); err != nil {
		t.Fatal(err)
	}
	if got := count(index.Query{Agent: "agent-y"}); !same(got, "rec_1") {
		t.Fatalf("facts on the new transcript: %v", got)
	}
	// Deleting the recording forgets its facts as well.
	if err := ix.DeleteRecording(ctx, "rec_1"); err != nil {
		t.Fatal(err)
	}
	if _, known, err := ix.GetFacts(ctx, "rec_1"); err != nil || known {
		t.Fatalf("facts survived the delete: %v %v", known, err)
	}
}
