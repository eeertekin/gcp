package storage

import (
	"net/http"
	"os"
	"testing"
	"time"
)

// Expected URLs are printed by testdata/hmac_signed_url.py.
func TestSignedURLHMACKnownAnswer(t *testing.T) {
	now := time.Date(2019, 2, 1, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		object string
		opts   SignedURLOptions
		want   string
	}{
		{
			object: "test-object",
			opts:   SignedURLOptions{Method: http.MethodGet, Expires: now.Add(15 * time.Second)},
			want:   "https://storage.googleapis.com/test-bucket/test-object?X-Goog-Algorithm=GOOG4-HMAC-SHA256&X-Goog-Credential=GOOG1EXAMPLEACCESSID%2F20190201%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20190201T090000Z&X-Goog-Expires=15&X-Goog-SignedHeaders=host&X-Goog-Signature=58c05e8f48c602164e3f472f7efac3af7cad2a4cbadbebc6aeb5ae96bccadc20",
		},
		{
			object: "dir/hello world&más.txt",
			opts: SignedURLOptions{
				Method:          http.MethodPut,
				Expires:         now.Add(time.Hour),
				ContentType:     "text/plain",
				QueryParameters: map[string][]string{"response-content-disposition": {"attachment"}},
			},
			want: "https://storage.googleapis.com/test-bucket/dir/hello%20world%26m%C3%A1s.txt?X-Goog-Algorithm=GOOG4-HMAC-SHA256&X-Goog-Credential=GOOG1EXAMPLEACCESSID%2F20190201%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20190201T090000Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=content-type%3Bhost&response-content-disposition=attachment&X-Goog-Signature=05554952f1965af919396fe3768dec58ab252e7e369d7983f142023267ec9036",
		},
	}
	for _, tc := range cases {
		tc.opts.GoogleAccessID = "GOOG1EXAMPLEACCESSID"
		tc.opts.HMACSecret = "bGoa+V7g/yqDXvKRqq+JTFn4uQZbPiQJo4pf9RzJ"
		got, err := signURL("test-bucket", tc.object, &tc.opts, "", now)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.object, got, tc.want)
		}
	}
}

func TestSignedURLHMACLive(t *testing.T) {
	bucket, object := os.Getenv("STORAGE_HMAC_TEST_BUCKET"), os.Getenv("STORAGE_HMAC_TEST_OBJECT")
	accessID, secret := os.Getenv("STORAGE_HMAC_TEST_ACCESS_ID"), os.Getenv("STORAGE_HMAC_TEST_SECRET")
	if bucket == "" || object == "" || accessID == "" || secret == "" {
		t.Skip("set STORAGE_HMAC_TEST_BUCKET, STORAGE_HMAC_TEST_OBJECT, STORAGE_HMAC_TEST_ACCESS_ID and STORAGE_HMAC_TEST_SECRET")
	}
	u, err := SignedURL(bucket, object, &SignedURLOptions{
		GoogleAccessID: accessID,
		HMACSecret:     secret,
		Method:         http.MethodGet,
		Expires:        time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %s", u, resp.Status)
	}
}
