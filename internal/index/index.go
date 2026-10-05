// Package index keeps every transcript line in Meilisearch and searches them.
//
// One document per line. The index is searched with typo tolerance on, because Hinglish is
// spelled many ways ("namaskar", "namaskaar"), and both layers are searchable, so a Devanagari
// query finds the same lines. One transcript per recording is indexed: the latest one.
package index

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/meilisearch/meilisearch-go"
)

// Line is one indexed transcript line.
type Line struct {
	ID           string  `json:"id"` // <transcript id>-<segment index>
	TranscriptID string  `json:"transcript_id"`
	RecordingID  string  `json:"recording_id"`
	WorkspaceID  string  `json:"workspace_id"`
	Index        uint32  `json:"idx"`
	StartSeconds float64 `json:"start"`
	EndSeconds   float64 `json:"end"`
	TextRoman    string  `json:"text_roman"`
	TextScript   string  `json:"text_script"`
	Language     string  `json:"language"`
	// CreatedAt is the transcript's creation time as Unix seconds, so a window can be filtered.
	CreatedAt int64 `json:"created_at"`
	// The recording's facts (likho.recording.updated), copied onto every line so a search can be
	// narrowed by them. CallTime is Unix seconds: the dialer's call time, else the recording's.
	Source      string `json:"source"`
	Campaign    string `json:"campaign"`
	Agent       string `json:"agent"`
	Disposition string `json:"disposition"`
	CallTime    int64  `json:"call_time"`
}

// Facts is what is known about a recording besides its lines.
type Facts struct {
	RecordingID string `json:"id"`
	WorkspaceID string `json:"workspace_id"`
	Source      string `json:"source"`
	Campaign    string `json:"campaign"`
	Agent       string `json:"agent"`
	Disposition string `json:"disposition"`
	CallTime    int64  `json:"call_time"`
}

// apply copies the facts onto a line.
func (f Facts) apply(line *Line) {
	line.Source, line.Campaign, line.Agent, line.Disposition, line.CallTime = f.Source, f.Campaign, f.Agent, f.Disposition, f.CallTime
}

// Hit is a matching line with the matches marked.
type Hit struct {
	Line
	HighlightRoman  string
	HighlightScript string
}

// Query is what a search asks for.
type Query struct {
	WorkspaceID string
	Text        string
	Language    string
	RecordingID string
	Since       time.Time // zero = no lower bound
	Until       time.Time // zero = no upper bound
	Page        int       // 1-based
	PageSize    int
	// The recording's facts: exact values, empty = any; a window on when the call happened.
	Source      string
	Campaign    string
	Agent       string
	Disposition string
	CallSince   time.Time
	CallUntil   time.Time
}

// Result is a page of hits.
type Result struct {
	Hits         []Hit
	Page         int
	PageSize     int
	Total        int
	ProcessingMs int
}

// Index is the connection to one Meilisearch index of lines, with a second one beside it that
// holds the facts of each recording (so lines indexed later get them, and lines indexed earlier
// can be given them).
type Index struct {
	client meilisearch.ServiceManager
	index  meilisearch.IndexManager
	facts  meilisearch.IndexManager
	name   string
}

// Open connects to Meilisearch and makes sure the indexes exist with the right settings.
func Open(ctx context.Context, url, apiKey, name string, maxHits int) (*Index, error) {
	client := meilisearch.New(url, meilisearch.WithAPIKey(apiKey))
	if _, err := client.HealthWithContext(ctx); err != nil {
		return nil, fmt.Errorf("meilisearch at %s: %w", url, err)
	}
	ix := &Index{client: client, index: client.Index(name), facts: client.Index(name + "_recordings"), name: name}
	if err := ix.ensure(ctx, maxHits); err != nil {
		return nil, err
	}
	return ix, nil
}

// ensure creates the indexes if needed and applies the settings (idempotent).
func (ix *Index) ensure(ctx context.Context, maxHits int) error {
	for _, name := range []string{ix.name, ix.name + "_recordings"} {
		if _, err := ix.client.GetIndexWithContext(ctx, name); err != nil {
			task, err := ix.client.CreateIndexWithContext(ctx, &meilisearch.IndexConfig{Uid: name, PrimaryKey: "id"})
			if err != nil {
				return fmt.Errorf("meilisearch: create index %s: %w", name, err)
			}
			if err := ix.wait(ctx, task.TaskUID); err != nil {
				return err
			}
		}
	}
	task, err := ix.index.UpdateSettingsWithContext(ctx, &meilisearch.Settings{
		SearchableAttributes: []string{"text_roman", "text_script"},
		FilterableAttributes: []string{
			"workspace_id", "recording_id", "transcript_id", "language", "created_at",
			"source", "campaign", "agent", "disposition", "call_time",
		},
		SortableAttributes:  []string{"created_at", "start", "call_time"},
		DisplayedAttributes: []string{"*"},
		Pagination:          &meilisearch.Pagination{MaxTotalHits: int64(maxHits)},
	})
	if err != nil {
		return fmt.Errorf("meilisearch: settings of %s: %w", ix.name, err)
	}
	if err := ix.wait(ctx, task.TaskUID); err != nil {
		return err
	}
	task, err = ix.facts.UpdateSettingsWithContext(ctx, &meilisearch.Settings{
		SearchableAttributes: []string{},
		FilterableAttributes: []string{"workspace_id"},
		DisplayedAttributes:  []string{"*"},
	})
	if err != nil {
		return fmt.Errorf("meilisearch: settings of %s_recordings: %w", ix.name, err)
	}
	return ix.wait(ctx, task.TaskUID)
}

// PutFacts stores what is known about a recording and gives its lines, if any are indexed, the
// same facts.
func (ix *Index) PutFacts(ctx context.Context, facts Facts) error {
	if facts.RecordingID == "" {
		return errors.New("a recording id is required")
	}
	task, err := ix.facts.AddDocumentsWithContext(ctx, []Facts{facts}, nil)
	if err != nil {
		return fmt.Errorf("meilisearch: facts of %s: %w", facts.RecordingID, err)
	}
	if err := ix.wait(ctx, task.TaskUID); err != nil {
		return err
	}
	return ix.applyFacts(ctx, facts)
}

// GetFacts returns what is known about a recording, or false when nothing is.
func (ix *Index) GetFacts(ctx context.Context, recordingID string) (Facts, bool, error) {
	var facts Facts
	err := ix.facts.GetDocumentWithContext(ctx, recordingID, nil, &facts)
	if err != nil {
		var meiliErr *meilisearch.Error
		if errors.As(err, &meiliErr) && meiliErr.StatusCode == 404 {
			return Facts{}, false, nil
		}
		return Facts{}, false, fmt.Errorf("meilisearch: facts of %s: %w", recordingID, err)
	}
	return facts, true, nil
}

// applyFacts patches every indexed line of the recording with the facts.
func (ix *Index) applyFacts(ctx context.Context, facts Facts) error {
	type patch struct {
		ID          string `json:"id"`
		Source      string `json:"source"`
		Campaign    string `json:"campaign"`
		Agent       string `json:"agent"`
		Disposition string `json:"disposition"`
		CallTime    int64  `json:"call_time"`
	}
	var patches []patch
	for offset := int64(0); ; offset += 1000 {
		var page meilisearch.DocumentsResult
		err := ix.index.GetDocumentsWithContext(ctx, &meilisearch.DocumentsQuery{
			Filter: "recording_id = " + quote(facts.RecordingID), Fields: []string{"id"}, Limit: 1000, Offset: offset,
		}, &page)
		if err != nil {
			return fmt.Errorf("meilisearch: lines of %s: %w", facts.RecordingID, err)
		}
		for _, raw := range page.Results {
			var line struct {
				ID string `json:"id"`
			}
			if err := raw.DecodeInto(&line); err != nil {
				return err
			}
			patches = append(patches, patch{line.ID, facts.Source, facts.Campaign, facts.Agent, facts.Disposition, facts.CallTime})
		}
		if int64(len(page.Results)) < 1000 {
			break
		}
	}
	if len(patches) == 0 {
		return nil
	}
	task, err := ix.index.UpdateDocumentsWithContext(ctx, patches, nil)
	if err != nil {
		return fmt.Errorf("meilisearch: update lines of %s: %w", facts.RecordingID, err)
	}
	return ix.wait(ctx, task.TaskUID)
}

// Ping reports whether Meilisearch answers.
func (ix *Index) Ping(ctx context.Context) error {
	_, err := ix.client.HealthWithContext(ctx)
	return err
}

// Replace stores the lines of one transcript, removing every line the recording had before.
// The recording's facts, when known, go onto every line.
func (ix *Index) Replace(ctx context.Context, recordingID string, lines []Line) error {
	if err := ix.deleteLines(ctx, recordingID); err != nil {
		return err
	}
	if len(lines) == 0 {
		return nil
	}
	facts, known, err := ix.GetFacts(ctx, recordingID)
	if err != nil {
		return err
	}
	if known {
		for i := range lines {
			facts.apply(&lines[i])
		}
	}
	task, err := ix.index.AddDocumentsWithContext(ctx, lines, nil)
	if err != nil {
		return fmt.Errorf("meilisearch: add lines of %s: %w", recordingID, err)
	}
	return ix.wait(ctx, task.TaskUID)
}

// DeleteRecording forgets every line of a recording, and its facts.
func (ix *Index) DeleteRecording(ctx context.Context, recordingID string) error {
	if err := ix.deleteLines(ctx, recordingID); err != nil {
		return err
	}
	task, err := ix.facts.DeleteDocumentWithContext(ctx, recordingID, nil)
	if err != nil {
		return fmt.Errorf("meilisearch: delete facts of %s: %w", recordingID, err)
	}
	return ix.wait(ctx, task.TaskUID)
}

func (ix *Index) deleteLines(ctx context.Context, recordingID string) error {
	task, err := ix.index.DeleteDocumentsByFilterWithContext(ctx, "recording_id = "+quote(recordingID), nil)
	if err != nil {
		return fmt.Errorf("meilisearch: delete lines of %s: %w", recordingID, err)
	}
	return ix.wait(ctx, task.TaskUID)
}

// Workspace returns the workspace a recording's lines belong to, or "" when none are indexed.
func (ix *Index) Workspace(ctx context.Context, recordingID string) (string, error) {
	var result meilisearch.DocumentsResult
	err := ix.index.GetDocumentsWithContext(ctx, &meilisearch.DocumentsQuery{
		Filter: "recording_id = " + quote(recordingID), Limit: 1, Fields: []string{"workspace_id"},
	}, &result)
	if err != nil {
		return "", fmt.Errorf("meilisearch: lines of %s: %w", recordingID, err)
	}
	if len(result.Results) == 0 {
		return "", nil
	}
	var line Line
	if err := result.Results[0].DecodeInto(&line); err != nil {
		return "", err
	}
	return line.WorkspaceID, nil
}

// Search returns the lines matching the query, best first.
func (ix *Index) Search(ctx context.Context, q Query) (Result, error) {
	if q.WorkspaceID == "" {
		return Result{}, errors.New("a workspace is required")
	}
	page, size := q.Page, q.PageSize
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	filter := []string{"workspace_id = " + quote(q.WorkspaceID)}
	if q.Language != "" {
		filter = append(filter, "language = "+quote(q.Language))
	}
	if q.RecordingID != "" {
		filter = append(filter, "recording_id = "+quote(q.RecordingID))
	}
	if !q.Since.IsZero() {
		filter = append(filter, fmt.Sprintf("created_at >= %d", q.Since.Unix()))
	}
	if !q.Until.IsZero() {
		filter = append(filter, fmt.Sprintf("created_at <= %d", q.Until.Unix()))
	}
	for field, value := range map[string]string{"source": q.Source, "campaign": q.Campaign, "agent": q.Agent, "disposition": q.Disposition} {
		if value != "" {
			filter = append(filter, field+" = "+quote(value))
		}
	}
	if !q.CallSince.IsZero() {
		filter = append(filter, fmt.Sprintf("call_time >= %d", q.CallSince.Unix()))
	}
	if !q.CallUntil.IsZero() {
		filter = append(filter, fmt.Sprintf("call_time <= %d", q.CallUntil.Unix()))
	}
	hitsPerPage := int64(size)
	response, err := ix.index.SearchWithContext(ctx, q.Text, &meilisearch.SearchRequest{
		Filter:                strings.Join(filter, " AND "),
		Page:                  int64(page),
		HitsPerPage:           &hitsPerPage,
		AttributesToHighlight: []string{"text_roman", "text_script"},
		HighlightPreTag:       "<mark>",
		HighlightPostTag:      "</mark>",
	})
	if err != nil {
		return Result{}, fmt.Errorf("meilisearch: search: %w", err)
	}
	result := Result{Page: page, PageSize: size, Total: int(response.TotalHits), ProcessingMs: int(response.ProcessingTimeMs)}
	for _, raw := range response.Hits {
		var hit Hit
		if err := raw.DecodeInto(&hit.Line); err != nil {
			return Result{}, err
		}
		var formatted struct {
			Formatted struct {
				TextRoman  string `json:"text_roman"`
				TextScript string `json:"text_script"`
			} `json:"_formatted"`
		}
		if err := raw.DecodeInto(&formatted); err == nil {
			hit.HighlightRoman = formatted.Formatted.TextRoman
			hit.HighlightScript = formatted.Formatted.TextScript
		}
		result.Hits = append(result.Hits, hit)
	}
	return result, nil
}

// Drop deletes both indexes (tests).
func (ix *Index) Drop(ctx context.Context) error {
	for _, name := range []string{ix.name, ix.name + "_recordings"} {
		task, err := ix.client.DeleteIndexWithContext(ctx, name)
		if err != nil {
			return err
		}
		if err := ix.wait(ctx, task.TaskUID); err != nil {
			return err
		}
	}
	return nil
}

func (ix *Index) wait(ctx context.Context, taskUID int64) error {
	task, err := ix.client.WaitForTaskWithContext(ctx, taskUID, 50*time.Millisecond)
	if err != nil {
		return fmt.Errorf("meilisearch: task %d: %w", taskUID, err)
	}
	if task.Status != meilisearch.TaskStatusSucceeded {
		return fmt.Errorf("meilisearch: task %d %s: %s", taskUID, task.Status, task.Error.Message)
	}
	return nil
}

// quote makes a filter value: double quotes, with inner quotes and backslashes escaped.
func quote(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value) + `"`
}
