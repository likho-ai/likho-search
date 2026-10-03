// Package app puts the service together and runs it.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"connectrpc.com/grpchealth"
	"github.com/likho-ai/likho-contracts/packages/go/gen/likho/search/v1/searchv1connect"

	"github.com/likho-ai/likho-search/internal/config"
	"github.com/likho-ai/likho-search/internal/events"
	"github.com/likho-ai/likho-search/internal/index"
	"github.com/likho-ai/likho-search/internal/indexer"
	"github.com/likho-ai/likho-search/internal/rpc"
)

// Version of the service, shown in the start-up log line.
const Version = "0.1.0"

// App is the running service.
type App struct {
	cfg     config.Config
	log     *slog.Logger
	index   *index.Index
	bus     *events.Bus
	indexer *indexer.Indexer

	httpListener net.Listener
	grpcListener net.Listener
	httpServer   *http.Server
	grpcServer   *http.Server
	health       *grpchealth.StaticChecker
}

// Options change how the service is put together. The zero value is the real thing.
type Options struct {
	// Transcripts replaces the likho-transcription client (tests).
	Transcripts indexer.Transcripts
}

// New connects to everything the service needs and opens its ports. Nothing is served
// until Run is called.
func New(ctx context.Context, cfg config.Config, log *slog.Logger, options Options) (*App, error) {
	ix, err := index.Open(ctx, cfg.MeiliURL, cfg.MeiliAPIKey, cfg.IndexName, cfg.MaxHits)
	if err != nil {
		return nil, err
	}
	bus, err := events.Connect(ctx, cfg.NATSURL)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*App, error) {
		bus.Close()
		return nil, err
	}
	httpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.HTTPPort))
	if err != nil {
		return fail(err)
	}
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		_ = httpListener.Close()
		return fail(err)
	}

	transcripts := options.Transcripts
	if transcripts == nil {
		transcripts = indexer.NewTranscriptsClient(cfg.TranscriptionGRPCAddr, cfg.RPCTimeout)
	}
	in := indexer.New(ix, transcripts, log)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if !bus.Connected() || ix.Ping(ctx) != nil {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})

	rpcMux := http.NewServeMux()
	rpcMux.Handle(searchv1connect.NewSearchServiceHandler(rpc.New(ix, in, log)))
	health := grpchealth.NewStaticChecker(searchv1connect.SearchServiceName)
	rpcMux.Handle(grpchealth.NewHandler(health))

	// gRPC clients speak HTTP/2 without TLS inside the cluster.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	return &App{
		cfg: cfg, log: log, index: ix, bus: bus, indexer: in,
		httpListener: httpListener,
		grpcListener: grpcListener,
		httpServer:   &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second},
		grpcServer:   &http.Server{Handler: rpcMux, Protocols: protocols, ReadHeaderTimeout: 10 * time.Second},
		health:       health,
	}, nil
}

// HTTPAddr is the address the HTTP side listens on ("127.0.0.1:4040").
func (a *App) HTTPAddr() string { return localAddr(a.httpListener) }

// GRPCAddr is the address the gRPC side listens on.
func (a *App) GRPCAddr() string { return localAddr(a.grpcListener) }

// Bus is the event bus connection (tests publish through it).
func (a *App) Bus() *events.Bus { return a.bus }

func localAddr(listener net.Listener) string {
	return fmt.Sprintf("127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port)
}

// Run serves until ctx is cancelled, then stops cleanly.
func (a *App) Run(ctx context.Context) error {
	failed := make(chan error, 2)
	serve := func(server *http.Server, listener net.Listener) {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failed <- err
		}
	}
	go serve(a.httpServer, a.httpListener)
	go serve(a.grpcServer, a.grpcListener)

	if a.cfg.ConsumersEnabled {
		if err := a.indexer.Listen(ctx, a.bus, a.cfg.ConsumerGroup); err != nil {
			return err
		}
	}
	a.log.Info(fmt.Sprintf("likho-search %s: HTTP on %s, gRPC on %s, index %q, consumers %s",
		Version, a.httpListener.Addr(), a.grpcListener.Addr(), a.cfg.IndexName, onOff(a.cfg.ConsumersEnabled)))

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-failed:
	}

	a.log.Info("stopping")
	a.health.SetStatus(searchv1connect.SearchServiceName, grpchealth.StatusNotServing)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = a.httpServer.Shutdown(shutdownCtx)
	_ = a.grpcServer.Shutdown(shutdownCtx)
	a.bus.Close()
	return runErr
}

func onOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}
