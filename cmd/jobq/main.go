// Command jobq runs the job queue server, a demo worker, or a load benchmark.
//
//	jobq serve  -addr :8080 -db ./jobq.db
//	jobq worker -url http://localhost:8080 -queue emails
//	jobq bench  -jobs 20000 -workers 32
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mishraashmit0025-lgtm/Durable-Job-Queue/client"
	"github.com/mishraashmit0025-lgtm/Durable-Job-Queue/queue"
	"github.com/mishraashmit0025-lgtm/Durable-Job-Queue/server"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: jobq {serve|worker|bench} [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "worker":
		err = worker(os.Args[2:])
	case "bench":
		err = bench(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "usage: jobq {serve|worker|bench} [flags]")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	path := fs.String("db", "./jobq.db", "database file")
	retain := fs.Duration("retain", 7*24*time.Hour, "delete succeeded jobs after this long (0 keeps them)")
	_ = fs.Parse(args)
	b, err := queue.Open(queue.Config{Path: *path, RetainSucceeded: *retain})
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: *addr, Handler: server.New(b), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)
		<-ch
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()
	log.Printf("jobq listening on %s (db %s)", *addr, *path)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return b.Close()
}

// worker is a demo consumer: it "sends" an email, failing 20% of the time.
func worker(args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	url := fs.String("url", "http://localhost:8080", "server URL")
	q := fs.String("queue", "emails", "queue to consume")
	conc := fs.Int("concurrency", 4, "parallel jobs")
	_ = fs.Parse(args)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	host, _ := os.Hostname()
	logger := log.New(os.Stderr, "", log.LstdFlags)
	logger.Printf("worker consuming %q from %s", *q, *url)
	return client.New(*url).Work(ctx, *q, func(ctx context.Context, j *queue.Job) (any, error) {
		time.Sleep(time.Duration(50+rand.IntN(200)) * time.Millisecond)
		if rand.IntN(5) == 0 {
			logger.Printf("job %s attempt %d failed", j.ID, j.Attempts)
			return nil, errors.New("smtp: temporary failure")
		}
		logger.Printf("job %s done (payload %s)", j.ID, j.Payload)
		return map[string]string{"status": "sent"}, nil
	}, client.WorkerOptions{Name: fmt.Sprintf("%s-%d", host, os.Getpid()), Concurrency: *conc, Logger: logger})
}

// bench runs the server in-process and measures end-to-end throughput over
// real HTTP: producers enqueue, workers lease in batches and ack each job.
func bench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	jobs := fs.Int("jobs", 20000, "jobs to process")
	producers := fs.Int("producers", 16, "concurrent producers")
	workers := fs.Int("workers", 32, "concurrent handlers (across 4 worker processes' worth of clients)")
	_ = fs.Parse(args)
	dir, err := os.MkdirTemp("", "jobq-bench")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	b, err := queue.Open(queue.Config{Path: filepath.Join(dir, "q.db")})
	if err != nil {
		return err
	}
	defer b.Close()
	ts := httptest.NewServer(server.New(b))
	defer ts.Close()
	c := client.New(ts.URL)
	c.HTTP.Transport = &http.Transport{MaxIdleConnsPerHost: 256}
	ctx := context.Background()

	t0 := time.Now()
	var next atomic.Int64
	var wg sync.WaitGroup
	for p := 0; p < *producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := next.Add(1); i <= int64(*jobs); i = next.Add(1) {
				if _, err := c.Enqueue(ctx, "bench", map[string]int64{"i": i}, client.EnqueueOptions{}); err != nil {
					log.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()
	enq := time.Since(t0)
	fmt.Printf("enqueue: %d jobs in %v (%.0f jobs/s, %d producers, every enqueue durable before reply)\n",
		*jobs, enq.Round(time.Millisecond), float64(*jobs)/enq.Seconds(), *producers)

	var done atomic.Int64
	wctx, cancel := context.WithCancel(ctx)
	t0 = time.Now()
	finished := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c.Work(wctx, "bench", func(context.Context, *queue.Job) (any, error) {
				if done.Add(1) == int64(*jobs) {
					close(finished)
				}
				return nil, nil
			}, client.WorkerOptions{Name: fmt.Sprint("bench-", w), Concurrency: *workers / 4})
		}(w)
	}
	<-finished
	proc := time.Since(t0)
	cancel()
	wg.Wait()
	fmt.Printf("lease+ack: %d jobs in %v (%.0f jobs/s, %d handlers)\n",
		*jobs, proc.Round(time.Millisecond), float64(*jobs)/proc.Seconds(), *workers)
	for _, s := range b.Stats() {
		fmt.Printf("stats: %+v\n", s)
	}
	return nil
}
