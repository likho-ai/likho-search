# likho-search

The search service of [Likho](https://github.com/likho-ai): every line of every transcript,
found by a few words in either layer - Hinglish (`namaskar`) or Devanagari (`नमस्कार`) - with
typo tolerance, because Hinglish is spelled many ways.

| It does | How |
| --- | --- |
| Keeps the index current | Listens to `likho.transcription.completed` and `likho.transcript.corrected`, fetches the transcript from likho-transcription, stores one document per line |
| Forgets deleted recordings | Listens to `likho.recording.deleted` |
| Answers searches | `Search(workspace, query, …)` - a page of lines, best first, matches wrapped in `<mark>` |
| Takes orders | `Reindex(transcript)`, `DeleteRecording(recording)` |

Go, [Connect](https://connectrpc.com) (which also answers plain gRPC), Meilisearch, NATS JetStream.

## What is indexed

One transcript per recording - the latest. A line is
`{id, transcript_id, recording_id, workspace_id, idx, start, end, text_roman, text_script, language, created_at}`;
`text_roman` and `text_script` are searched, the rest filters (workspace, recording, language,
a time window). A search is always inside one workspace. Names, dialer ids and the other facts
about a recording are likho-api's: it decorates the hits it gets from here.

## Run it

```bash
# the likho-infra stack running (Meilisearch, NATS) and likho-transcription on :5020
go run ./cmd/likho-search
```

Settings: `.env.development`, `.env.staging`, `.env.production` (read as the other services
do; `.env.<environment>.local` for secrets).

| Variable | Default | Meaning |
| --- | --- | --- |
| `HTTP_PORT`, `GRPC_PORT` | 4040, 5040 | `/healthz` and `/readyz`; `likho.search.v1.SearchService` |
| `NATS_URL` | `nats://localhost:4222` | The event bus |
| `NATS_CONNECT_TIMEOUT_SECONDS` | `120` | How long the start keeps trying to reach NATS before giving up |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty | Also push the metrics there (OTLP/HTTP); `GET /metrics` (calls by method and status with how long they took, transcripts and lines indexed) is always on |
| `MEILI_URL`, `MEILI_API_KEY` | the local stack's | Meilisearch (the key is refused as a development one in staging and production) |
| `INDEX_NAME` | `segments` | The index |
| `TRANSCRIPTION_GRPC_ADDR` | `localhost:5020` | Where transcripts are fetched from |
| `CONSUMERS_ENABLED`, `CONSUMER_GROUP` | `true`, `likho-search` | Off for an instance that only answers searches |
| `MAX_HITS` | 10000 | The most lines one search can page through |

## Develop

```bash
go test ./...        # against the local stack (skipped without it; CI insists)
go vet ./... && gofmt -l .
docker build -t likho-search .
```

The tests put lines into an index of their own and search them - typos, both layers, filters,
paging, a newer transcript replacing an older one - and run the whole service against the
real NATS with a stand-in likho-transcription: events in, lines searchable over gRPC, a
deleted recording gone.
