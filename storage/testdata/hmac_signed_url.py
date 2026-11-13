#!/usr/bin/env python3
# Expected URLs for TestSignedURLHMACKnownAnswer, computed independently of the
# Go implementation from https://cloud.google.com/storage/docs/authentication/signatures
import hashlib
import hmac
from urllib.parse import quote

ACCESS_ID = "GOOG1EXAMPLEACCESSID"
SECRET = "bGoa+V7g/yqDXvKRqq+JTFn4uQZbPiQJo4pf9RzJ"
TIMESTAMP = "20190201T090000Z"
DATE = TIMESTAMP[:8]
SCOPE = f"{DATE}/auto/storage/goog4_request"


def enc(s):
    return quote(s, safe="-_.~")


def mac(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def sign(method, bucket, obj, expires, headers, params):
    host = "storage.googleapis.com"
    path = f"/{bucket}/" + "/".join(enc(seg) for seg in obj.split("/"))
    headers = {k.lower(): v for k, v in headers.items()}
    headers["host"] = host
    signed = ";".join(sorted(headers))
    query = dict(params)
    query.update({
        "X-Goog-Algorithm": "GOOG4-HMAC-SHA256",
        "X-Goog-Credential": f"{ACCESS_ID}/{SCOPE}",
        "X-Goog-Date": TIMESTAMP,
        "X-Goog-Expires": str(expires),
        "X-Goog-SignedHeaders": signed,
    })
    canonical_query = "&".join(f"{enc(k)}={enc(v)}" for k, v in sorted(query.items()))
    canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
    canonical = "\n".join([method, path, canonical_query, canonical_headers, signed, "UNSIGNED-PAYLOAD"])
    string_to_sign = "\n".join([
        "GOOG4-HMAC-SHA256", TIMESTAMP, SCOPE,
        hashlib.sha256(canonical.encode()).hexdigest(),
    ])
    k_date = mac(("GOOG4" + SECRET).encode(), DATE)
    k_location = mac(k_date, "auto")
    k_service = mac(k_location, "storage")
    k_signing = mac(k_service, "goog4_request")
    signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
    return f"https://{host}{path}?{canonical_query}&X-Goog-Signature={signature}"


print(sign("GET", "test-bucket", "test-object", 15, {}, {}))
print(sign("PUT", "test-bucket", "dir/hello world&más.txt", 3600,
           {"Content-Type": "text/plain"}, {"response-content-disposition": "attachment"}))
