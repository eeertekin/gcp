package storage

import "time"

func SetIAMEndpoint(c *Client, endpoint string) { c.iamEndpoint = endpoint }

func SignedURLAt(bucket, object string, opts *SignedURLOptions, now time.Time) (string, error) {
	return signURL(bucket, object, opts, "", now)
}
