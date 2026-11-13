# Lightweight GCP clients (stdlib only)

Drop-in replacements for the parts of the official Google Cloud Go SDKs we actually use — **BigQuery**, **Cloud Storage** and **Pub/Sub** — written with nothing but the Go standard library.

The public API mirrors the official packages (`cloud.google.com/go/bigquery`, `cloud.google.com/go/storage`, `cloud.google.com/go/pubsub/v2`), so migrating is mostly an import change. `go.mod` has **zero** requirements.

## Results

Same program using all three services (BigQuery dataset metadata/create/load/copy/query/read rows/list datasets and tables/delete, Storage upload/signed URL/delete, Pub/Sub publish/receive), built with Go 1.27 for `linux/amd64` (`-trimpath`, `CGO_ENABLED=0`) against bigquery v1.86.2, storage v1.70.0, pubsub/v2 v2.9.0 and google.golang.org/api v0.314.0. Only the import paths differ between the two builds. Sizes are MiB; cold build is the median of three builds with an empty `GOCACHE`, cross-compiled on a 4-core arm64 host:

| | Official SDKs | stdlib | Difference |
|---|---|---|---|
| Stripped binary (`-s -w`) | 39.36 MB | **7.05 MB** | 5.6× smaller |
| Default binary | 57.49 MB | **10.33 MB** | 5.6× smaller |
| Modules (`go list -m all`) | 164 | **1** | |
| Modules linked | 59 | **1** | |
| Non-standard packages | 534 | **8** | |
| Cold build | 61.8 s | **16.4 s** | 3.8× faster |

Per service (stripped / default binary):

| Service | Official SDK | stdlib |
|---|---|---|
| Storage only | 32.40 MB / 47.39 MB | 6.82 MB / 10.01 MB |
| Pub/Sub only | 16.82 MB / 24.64 MB | 6.72 MB / 9.86 MB |
| BigQuery only | 22.13 MB / 32.41 MB | 6.76 MB / 9.92 MB |
| All three | 39.36 MB / 57.49 MB | 7.05 MB / 10.33 MB |

The SDKs grow with every service because each brings its own heavy dependencies (gRPC, protobuf, OpenTelemetry, Apache Arrow, …). The stdlib packages share `net/http` and `crypto`, so adding a service costs only its own logic (~0.1–0.3 MB).

## Layout

```
option/            shared client options (credentials, endpoint, HTTP client)
civil/             DATE / TIME / DATETIME values (mirrors cloud.google.com/go/civil)
googleapi/         HTTP error type (mirrors google.golang.org/api/googleapi)
iterator/          iterator.Done (mirrors google.golang.org/api/iterator)
bigquery/          BigQuery REST client
└── bigquerytest/  in-process fake server for tests
storage/           Cloud Storage JSON API client + V4 signed URLs
└── storagetest/   in-process fake server for tests
pubsub/            Pub/Sub REST client (publish + subscribe)
└── pubsubtest/    in-process fake server for tests
```

Each service package is self-contained (its own auth and retry code), so you can copy a single folder into another project together with `option` (BigQuery also needs `civil`, `googleapi` and `iterator`).

## Authentication

All three clients accept a **service account** key or **user credentials**. For a service account, a JWT is signed with RS256 and exchanged for an OAuth2 access token; user credentials use their refresh token, and their `quota_project_id` is sent as `X-Goog-User-Project`. Tokens are cached and refreshed one minute before expiry.

When no credentials option is given, every client uses **Application Default Credentials**: `GOOGLE_APPLICATION_CREDENTIALS`, then the gcloud file from `gcloud auth application-default login` (service account or user credentials), then the GCE / Cloud Run / GKE metadata server. `DetectProjectID` takes the project from whichever credentials were found.

Storage signed URLs are signed locally with a service account key. With metadata-server credentials they are signed through the IAM Credentials `signBlob` API as the VM's service account (it needs `iam.serviceAccounts.signBlob` on itself, e.g. Service Account Token Creator, and the `cloud-platform` scope). With user credentials, `SignedURL` returns an error unless `GoogleAccessID` and `PrivateKey` or `SignBytes` are set. With an **HMAC key** (`GoogleAccessID` set to the key's access ID and `HMACSecret` to its secret), URLs are signed locally with `GOOG4-HMAC-SHA256`: no client, no RSA key, no network call, whatever the client's credentials are.

```go
import "github.com/eeertekin/gcp/option"

option.WithAuthCredentialsJSON(option.ServiceAccount, jsonBytes)
option.WithAuthCredentialsFile(option.ServiceAccount, "/path/to/key.json")
option.WithAuthCredentialsJSON(option.AuthorizedUser, userJSON)
option.WithEndpoint("https://custom-endpoint")                  // emulators, fakes, private endpoints
option.WithHTTPClient(httpClient)                               // custom transport; auth is still added
```

## Cloud Storage — `github.com/eeertekin/gcp/storage`

```go
client, err := storage.NewClient(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, creds))
defer client.Close()

w := client.Bucket("my-bucket").Object("path/file.csv").NewWriter(ctx)
w.ContentType = "text/csv"
w.ContentDisposition = `attachment; filename="file.csv"`
io.Copy(w, f)
err = w.Close()

err = client.Bucket("my-bucket").Object("path/file.csv").Delete(ctx) // storage.ErrObjectNotExist on 404

url, err := client.Bucket("my-bucket").SignedURL("path/file.csv", &storage.SignedURLOptions{
	Scheme:  storage.SigningSchemeV4,
	Method:  "GET",
	Expires: time.Now().Add(15 * time.Minute),
})

url, err = storage.SignedURL("my-bucket", "path/file.csv", &storage.SignedURLOptions{
	GoogleAccessID: hmacAccessID, // GOOG1E...
	HMACSecret:     hmacSecret,
	Method:         "GET",
	Expires:        time.Now().Add(15 * time.Second),
})
```

- **Uploads:** small objects use a single media upload (multipart when `Writer.Metadata`, `Writer.ContentEncoding` or `Writer.ContentDisposition` is set); larger objects use resumable uploads in chunks (`Writer.ChunkSize`, default 16 MiB, aligned to 256 KiB). Interrupted chunks resume from the server's committed offset.
- **Objects:** `Attrs` (including `ContentType`, `ContentEncoding`, `ContentDisposition` and `Metadata`), `NewReader` / `NewRangeReader`, `Delete`, `Objects` with `Query{Prefix, Delimiter}`, `ComposerFrom`, `CopierFrom` (set `ContentType`, `ContentEncoding`, `ContentDisposition` or `Metadata` on the `Copier` to replace the destination's metadata instead of inheriting the source's). Preconditions with `If(storage.Conditions{DoesNotExist: true})` or `{GenerationMatch: gen}`, and `Generation(gen)`; a failed precondition is a `*storage.APIError` with `Code` 412.
- **gRPC and Rapid (zonal) buckets:** `storage.NewGRPCClient` speaks the `google.storage.v2` gRPC API over `net/http`'s HTTP/2 (protobuf encoded by hand), still without dependencies. It covers what zonal buckets offer: appendable writes (`Writer.Append`, `Flush`, `FinalizeOnClose`, takeover with `NewWriterFromAppendableObject`), bidi reads (`BidiReadObject`, which also reads unfinalized objects), `Attrs`, `Objects` and `Delete`, with redirects and routing tokens for writes and reads. Plain uploads, compose and copy are not available over gRPC. gRPC errors map to `*storage.APIError` with the HTTP equivalent in `Code` and the gRPC code name in `Reason`. Unlike the official SDK, `FinalizeOnClose` is read when `Close` runs, so it can be set after the first write.

```go
client, err := storage.NewGRPCClient(ctx)
w := client.Bucket("my-rapid-bucket").Object("log").If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
w.Append = true
w.Write(frame)
persisted, err := w.Flush() // durable and readable
w.FinalizeOnClose = true
err = w.Close()
```
- **Signed URLs:** V4 (`GOOG4-RSA-SHA256`, or `GOOG4-HMAC-SHA256` with `HMACSecret`) for any method, with headers, query parameters, content type/MD5, path-style, virtual-hosted-style or bucket-bound hostnames. Verified against Google's official conformance test suite (26/29 cases; the 3 skipped cases cover emulator hosts and non-default universe domains).
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

q := client.Query("SELECT id, name FROM demo")
q.DefaultDatasetID = "demo"
job, err = q.Run(ctx)
status, err = job.Wait(ctx)
it, err := job.Read(ctx)
for {
	var row []bigquery.Value
	err := it.Next(&row)
	if errors.Is(err, iterator.Done) {
		break
	}
	// row[0] is int64, row[1] is string; it.Schema describes the columns
}
```

- Queries: `Query.Run` uses `jobs.query`, so a short query costs one round trip; longer ones long-poll `getQueryResults` and pages are prefetched. Retries reuse the `requestId`, so a query never runs twice. `Job.Cancel`, and DML counts via `QueryStatistics.NumDMLAffectedRows`.
- Values decode like the official client: `int64`, `float64`, `bool`, `string`, `[]byte`, `*big.Rat` (NUMERIC/BIGNUMERIC), `civil.Date`/`Time`/`DateTime`, UTC `time.Time`, `*IntervalValue`, `*RangeValue`, and `[]bigquery.Value` for arrays and structs.
- Catalog: `Datasets`, `Dataset.Metadata`, `Dataset.Tables`.
- Errors: HTTP failures are `*googleapi.Error` (`Code`, `Message`, `Errors[].Reason`).
- Tables: `Create`, `Metadata`, `Delete`.
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
| `bigquery` | 59 (incl. paging, deadline-bounded polling, cancel, idempotent retry, ADC) |
| `storage` | 85 (incl. Google's V4 signing conformance suite, HMAC signed URL known answers, ADC, JSON object operations, and a gRPC suite that runs against both `storagetest` and a real gRPC server) |
| `pubsub` | 46 (incl. a 600-message end-to-end publish → receive, ADC) |

```sh
go test -race -timeout 15s ./...
```

The gRPC suite can also run against an external `google.storage.v2` server: `STORAGE_GRPC_TEST_ENDPOINT=http://127.0.0.1:9100 STORAGE_GRPC_TEST_BUCKET=<bucket> go test ./storage -run TestGRPCConformance`.

`storagetest` verifies HMAC signed URLs made with the key from `Server.HMACKey()`; they can also be checked against real GCS: `STORAGE_HMAC_TEST_BUCKET=<bucket> STORAGE_HMAC_TEST_OBJECT=<object> STORAGE_HMAC_TEST_ACCESS_ID=<access id> STORAGE_HMAC_TEST_SECRET=<secret> go test ./storage -run TestSignedURLHMACLive`.

The suites were mutation-checked: deliberately breaking core behaviors (resumable upload probing, signed content type, lease extension, flow control, ordered publishing, nack-on-shutdown, deadline-bounded query polling) makes the corresponding tests fail.
