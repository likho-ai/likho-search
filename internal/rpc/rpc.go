// Package rpc serves likho.search.v1.SearchService.
package rpc

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	searchv1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/search/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/likho-ai/likho-search/internal/index"
	"github.com/likho-ai/likho-search/internal/indexer"
)

// Service answers the calls.
type Service struct {
	index   *index.Index
	indexer *indexer.Indexer
	log     *slog.Logger
}

// New returns the service.
func New(ix *index.Index, in *indexer.Indexer, log *slog.Logger) *Service {
	return &Service{index: ix, indexer: in, log: log}
}

func invalid(message string) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(message))
}

// Search returns the lines matching the query in one workspace, best first.
func (s *Service) Search(ctx context.Context, req *connect.Request[searchv1.SearchRequest]) (*connect.Response[searchv1.SearchResponse], error) {
	msg := req.Msg
	if msg.GetWorkspaceId() == "" {
		return nil, invalid("workspace_id is required")
	}
	if msg.GetQuery() == "" {
		return nil, invalid("query is required")
	}
	query := index.Query{
		WorkspaceID: msg.GetWorkspaceId(),
		Text:        msg.GetQuery(),
		Language:    msg.GetLanguage(),
		RecordingID: msg.GetRecordingId(),
		Page:        int(msg.GetPage()),
		PageSize:    int(msg.GetPageSize()),
	}
	if msg.GetSince() != nil {
		query.Since = msg.GetSince().AsTime()
	}
	if msg.GetUntil() != nil {
		query.Until = msg.GetUntil().AsTime()
	}
	result, err := s.index.Search(ctx, query)
	if err != nil {
		s.log.Error("search failed", "error", err)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("the search index did not answer"))
	}
	response := &searchv1.SearchResponse{
		Page:         uint32(result.Page),
		PageSize:     uint32(result.PageSize),
		Total:        uint32(result.Total),
		ProcessingMs: uint32(result.ProcessingMs),
	}
	for _, hit := range result.Hits {
		response.Hits = append(response.Hits, &searchv1.Hit{
			RecordingId:     hit.RecordingID,
			TranscriptId:    hit.TranscriptID,
			SegmentIndex:    hit.Index,
			StartSeconds:    hit.StartSeconds,
			EndSeconds:      hit.EndSeconds,
			TextRoman:       hit.TextRoman,
			TextScript:      hit.TextScript,
			HighlightRoman:  hit.HighlightRoman,
			HighlightScript: hit.HighlightScript,
			Language:        hit.Language,
			CreatedAt:       timestamppb.New(unixTime(hit.CreatedAt)),
		})
	}
	return connect.NewResponse(response), nil
}

// Reindex fetches a transcript again and replaces the recording's lines with it.
func (s *Service) Reindex(ctx context.Context, req *connect.Request[searchv1.ReindexRequest]) (*connect.Response[searchv1.ReindexResponse], error) {
	if req.Msg.GetTranscriptId() == "" || req.Msg.GetWorkspaceId() == "" {
		return nil, invalid("transcript_id and workspace_id are required")
	}
	lines, err := s.indexer.Reindex(ctx, req.Msg.GetTranscriptId(), req.Msg.GetWorkspaceId())
	if err != nil {
		var connectErr *connect.Error
		if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeNotFound {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("no such transcript"))
		}
		s.log.Error("reindex failed", "transcript", req.Msg.GetTranscriptId(), "error", err)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("the transcript could not be indexed"))
	}
	return connect.NewResponse(&searchv1.ReindexResponse{Lines: uint32(lines)}), nil
}

// DeleteRecording forgets every line of a recording.
func (s *Service) DeleteRecording(ctx context.Context, req *connect.Request[searchv1.DeleteRecordingRequest]) (*connect.Response[searchv1.DeleteRecordingResponse], error) {
	if req.Msg.GetRecordingId() == "" {
		return nil, invalid("recording_id is required")
	}
	if err := s.index.DeleteRecording(ctx, req.Msg.GetRecordingId()); err != nil {
		s.log.Error("delete failed", "recording", req.Msg.GetRecordingId(), "error", err)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("the search index did not answer"))
	}
	return connect.NewResponse(&searchv1.DeleteRecordingResponse{}), nil
}

func unixTime(seconds int64) time.Time { return time.Unix(seconds, 0).UTC() }
