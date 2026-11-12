# Lightweight GCP clients (stdlib only)

Drop-in replacements for the parts of the official Google Cloud Go SDKs we actually use — **BigQuery**, **Cloud Storage** and **Pub/Sub** — written with nothing but the Go standard library.

The public API mirrors the official packages (`cloud.google.com/go/bigquery`, `cloud.google.com/go/storage`, `cloud.google.com/go/pubsub/v2`), so migrating is mostly an import change. `go.mod` has **zero** requirements.

## Results

Same program using all three services (BigQuery create/load/copy/delete, Storage upload/signed URL/delete, Pub/Sub publish/receive), built for `linux/amd64`:

| | Official SDKs | stdlib | Difference |
|---|---|---|---|
| Stripped binary (`-s -w`) | 39.04 MB | **6.85 MB** | 5.7× smaller |
| Default binary | 57.01 MB | **10.04 MB** | 5.7× smaller |
| Modules | 254 | **1** | |
| Non-standard packages | 529 | **5** | |
| Cold build | 18 s | **5 s** | 3.6× faster |

Per service (stripped / default binary):

| Service | Official SDK | stdlib |
|---|---|---|
| Storage only | 32.12 MB / 46.95 MB | 6.68 MB / 9.81 MB |
| Pub/Sub only | 17.15 MB / 25.07 MB | 6.71 MB / 9.84 MB |
| All three | 39.04 MB / 57.01 MB | 6.85 MB / 10.04 MB |

The SDKs grow with every service because each brings its own heavy dependencies (gRPC, protobuf, OpenTelemetry, Apache Arrow, …). The stdlib packages share `net/http` and `crypto`, so adding a service costs only its own logic (~0.1 MB).

## Layout

```
option/            shared client options (credentials, endpoint)
bigquery/          BigQuery REST client
└── bigquerytest/  in-process fake server for tests
storage/           Cloud Storage JSON API client + V4 signed URLs
└── storagetest/   in-process fake server for tests
pubsub/            Pub/Sub REST client (publish + subscribe)
└── pubsubtest/    in-process fake server for tests
```

Each service package is self-contained (its own auth and retry code), so you can copy a single folder into another project together with `option`.

## Authentication

All clients use a **service account** key. A JWT is signed with RS256 and exchanged for an OAuth2 access token, which is cached and refreshed one minute before expiry.

```go
import "github.com/eeertekin/gcp/option"

option.WithAuthCredentialsJSON(option.ServiceAccount, jsonBytes)
option.WithAuthCredentialsFile(option.ServiceAccount, "/path/to/key.json")
option.WithEndpoint("https://custom-endpoint")   // emulators, fakes, private endpoints
```

## Cloud Storage — `github.com/eeertekin/gcp/storage`

```go
client, err := storage.NewClient(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, creds))
defer client.Close()

w := client.Bucket("my-bucket").Object("path/file.csv").NewWriter(ctx)
w.ContentType = "text/csv"
io.Copy(w, f)
err = w.Close()

err = client.Bucket("my-bucket").Object("path/file.csv").Delete(ctx) // storage.ErrObjectNotExist on 404

url, err := client.Bucket("my-bucket").SignedURL("path/file.csv", &storage.SignedURLOptions{
	Scheme:  storage.SigningSchemeV4,
	Method:  "GET",
	Expires: time.Now().Add(15 * time.Minute),
})
```

- **Uploads:** small objects use a single media upload; larger objects use resumable uploads in chunks (`Writer.ChunkSize`, default 16 MiB, aligned to 256 KiB). Interrupted chunks resume from the server's committed offset.
- **Signed URLs:** V4 (`GOOG4-RSA-SHA256`) for any method, with headers, query parameters, content type/MD5, path-style, virtual-hosted-style or bucket-bound hostnames. Verified against Google's official conformance test suite (26/29 cases; the 3 skipped cases cover emulator hosts and non-default universe domains).
- **Retries:** exponential backoff with jitter on transport errors, 408/429/5xx, and token-exchange failures.

## BigQuery — `github.com/eeertekin/gcp/bigquery`

```go
client, err := bigquery.NewClient(ctx, bigquery.DetectProjectID, option.WithAuthCredentialsFile(option.ServiceAccount, path))
defer client.Close()

ds := client.Dataset("demo")
err = ds.Table("demo").Create(ctx, &bigquery.TableMetadata{
	Schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
})

ref := bigquery.NewGCSReference("gs://my-bucket/demo.csv")
ref.SourceFormat = bigquery.CSV
loader := ds.Table("demo").LoaderFrom(ref)
loader.WriteDisposition = bigquery.WriteTruncate
job, err := loader.Run(ctx)
status, err := job.Wait(ctx)

job, err = ds.Table("demo_copy").CopierFrom(ds.Table("demo")).Run(ctx)
err = ds.Table("demo").Delete(ctx) // bigquery.IsNotFound(err) on 404
```

- Tables: `Create`, `Delete`.
- Jobs: load from GCS (`LoaderFrom`), table copy (`CopierFrom`), `Status`, `Wait` (polling with backoff), job ID / location control via `JobIDConfig`.
- Job inserts are safe to retry: if a retried insert hits "already exists", the client attaches to the existing job instead of failing.

## Pub/Sub — `github.com/eeertekin/gcp/pubsub`

```go
client, err := pubsub.NewClient(ctx, pubsub.DetectProjectID, option.WithAuthCredentialsFile(option.ServiceAccount, path))
defer client.Close()

p := client.Publisher("demo")
id, err := p.Publish(ctx, &pubsub.Message{Data: []byte("hello")}).Get(ctx)
p.Stop()

sub := client.Subscriber("demo-sub")
err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
	m.Ack()
})
```

**Publisher**

- Batching by `CountThreshold`, `ByteThreshold` and `DelayThreshold` (same defaults as the SDK); batches never exceed the 1,000-message / 10 MB request limits.
- Flow control: `FlowControlIgnore`, `FlowControlBlock`, `FlowControlSignalError`.
- `NumGoroutines` caps concurrent requests; `Timeout` bounds each publish including retries.
- Ordering keys (`EnableMessageOrdering`): one in-flight batch per key, in order. A failure pauses the key (`ErrPublishingPaused`) until `ResumePublish`.
- `Flush`, `Stop`, `ErrPublisherStopped`, `ErrOversizedMessage`.

**Subscriber**

- `MaxOutstandingMessages` / `MaxOutstandingBytes` (negative = unlimited), `NumGoroutines` concurrent pull loops.
- Automatic ack-deadline extension up to `MaxExtension`.
- Acks and nacks are batched (flushed every 100 ms or every 500 IDs).
- Messages with the same ordering key are processed one at a time, in order.
- `ShutdownOptions`: wait for callbacks (default), return immediately (`Timeout: 0`), `ShutdownBehaviorNackImmediately`, or wait with a timeout.
- Transient pull errors are retried forever; permanent errors (e.g. `NOT_FOUND`) end `Receive` with that error.

**Differences from the official SDK**

- Uses REST pull instead of gRPC streaming pull (gRPC isn't practical with stdlib only). Slightly more latency per round-trip, otherwise equivalent.
- Not implemented: `AckWithResult` / exactly-once delivery, compression, per-stream flow control.
- Publish retries are bounded by `Client.Retry.MaxAttempts` (default 10) as well as `PublishSettings.Timeout`.
- On shutdown, messages still queued behind a busy ordering key are nacked for redelivery.

## Testing

Each package ships an in-process fake server (`bigquerytest`, `storagetest`, `pubsubtest`) that verifies JWT signatures, enforces protocol rules and supports fault injection (HTTP errors, dropped connections, failures after the request was applied). Use them in your own tests:

```go
srv, client := pubsubtest.NewServer(t)
srv.CreateTopic("t")
srv.CreateSubscription("s", "t", pubsubtest.SubscriptionConfig{})
```

| Package | Tests |
|---|---|
| `bigquery` | 34 |
| `storage` | 50 (incl. Google's V4 signing conformance suite) |
| `pubsub` | 42 (incl. a 600-message end-to-end publish → receive) |

```sh
go test -race -timeout 15s ./...
```

The suites were mutation-checked: deliberately breaking core behaviors (resumable upload probing, signed content type, lease extension, flow control, ordered publishing, nack-on-shutdown) makes the corresponding tests fail.
