package storagetest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

type listEntry struct {
	name   string
	obj    *Object
	prefix string
}

func (s *Server) listPage(bucket, prefix, delimiter, token string) ([]listEntry, string) {
	var names []string
	for key := range s.objects {
		b, name, _ := strings.Cut(key, "/")
		if b == bucket && strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	var entries []listEntry
	for _, name := range names {
		if i := strings.Index(name[len(prefix):], delimiter); delimiter != "" && i >= 0 {
			p := name[:len(prefix)+i+len(delimiter)]
			if len(entries) == 0 || entries[len(entries)-1].prefix != p {
				entries = append(entries, listEntry{name: p, prefix: p})
			}
			continue
		}
		entries = append(entries, listEntry{name: name, obj: s.objects[bucket+"/"+name]})
	}

	start, _ := strconv.Atoi(token)
	entries = entries[min(start, len(entries)):]
	size := s.pageSize
	if size <= 0 {
		size = 1000
	}
	if len(entries) <= size {
		return entries, ""
	}
	return entries[:size], strconv.Itoa(start + size)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	bucket, q := r.PathValue("b"), r.URL.Query()
	s.calls = append(s.calls, "LIST "+bucket+"/"+q.Get("prefix"))
	if !s.buckets[bucket] {
		writeAPIError(w, http.StatusNotFound, "notFound", "The specified bucket does not exist.")
		return
	}
	entries, next := s.listPage(bucket, q.Get("prefix"), q.Get("delimiter"), q.Get("pageToken"))
	items, prefixes := []any{}, []string{}
	for _, e := range entries {
		if e.obj != nil {
			items = append(items, objectJSON(bucket, e.name, e.obj))
		} else {
			prefixes = append(prefixes, e.prefix)
		}
	}
	body := map[string]any{"kind": "storage#objects", "items": items, "prefixes": prefixes}
	if next != "" {
		body["nextPageToken"] = next
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(body)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	bucket, name := r.PathValue("b"), r.PathValue("o")
	key := bucket + "/" + name
	if !s.exists(w, r, key) || !s.precondition(w, r.URL.Query(), key) {
		return
	}
	obj := s.objects[key]
	if r.URL.Query().Get("alt") != "media" {
		s.calls = append(s.calls, "GET "+key)
		s.writeObject(w, bucket, name, obj)
		return
	}
	s.calls = append(s.calls, "READ "+key+" "+r.Header.Get("Range"))
	data := obj.Data
	rng := r.Header.Get("Range")
	if rng == "" {
		w.Write(data)
		return
	}
	spec, ok := strings.CutPrefix(rng, "bytes=")
	a, b, ok2 := strings.Cut(spec, "-")
	first, err := strconv.ParseInt(a, 10, 64)
	if !ok || !ok2 || err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid", "bad Range "+rng)
		return
	}
	last := int64(len(data)) - 1
	if b != "" {
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n < first {
			writeAPIError(w, http.StatusBadRequest, "invalid", "bad Range "+rng)
			return
		}
		last = min(last, n)
	}
	if first >= int64(len(data)) {
		writeAPIError(w, http.StatusRequestedRangeNotSatisfiable, "requestedRangeNotSatisfiable", "The requested range cannot be satisfied.")
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(data)))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(data[first : last+1])
}

func (s *Server) handleCompose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SourceObjects []struct {
			Name       string `json:"name"`
			Generation string `json:"generation"`
		} `json:"sourceObjects"`
		Destination objectMeta `json:"destination"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	bucket, name := r.PathValue("b"), r.PathValue("o")
	s.calls = append(s.calls, "COMPOSE "+bucket+"/"+name)
	if len(req.SourceObjects) == 0 || len(req.SourceObjects) > 32 {
		writeAPIError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("compose needs 1 to 32 sources, got %d", len(req.SourceObjects)))
		return
	}
	if !s.precondition(w, r.URL.Query(), bucket+"/"+name) {
		return
	}
	var data []byte
	for _, src := range req.SourceObjects {
		obj, ok := s.objects[bucket+"/"+src.Name]
		if !ok || src.Generation != "" && src.Generation != strconv.FormatInt(obj.Generation, 10) {
			writeAPIError(w, http.StatusNotFound, "notFound", "No such object: "+bucket+"/"+src.Name)
			return
		}
		data = append(data, obj.Data...)
	}
	s.writeObject(w, bucket, name, s.store(bucket, name, req.Destination.ContentType, req.Destination.Metadata, data))
}

func (s *Server) handleRewrite(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	q := r.URL.Query()
	src := r.PathValue("b") + "/" + r.PathValue("o")
	bucket, name := r.PathValue("db"), r.PathValue("do")
	s.calls = append(s.calls, "REWRITE "+src+" "+bucket+"/"+name+" "+q.Get("rewriteToken"))
	obj, ok := s.objects[src]
	if g := q.Get("sourceGeneration"); ok && g != "" && g != strconv.FormatInt(obj.Generation, 10) {
		ok = false
	}
	if !ok {
		writeAPIError(w, http.StatusNotFound, "notFound", "No such object: "+src)
		return
	}
	if !s.precondition(w, q, bucket+"/"+name) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if q.Get("rewriteToken") == "" {
		json.NewEncoder(w).Encode(map[string]any{"kind": "storage#rewriteResponse", "done": false, "rewriteToken": "step-1"})
		return
	}
	dst := s.store(bucket, name, obj.ContentType, obj.Metadata, slices.Clone(obj.Data))
	json.NewEncoder(w).Encode(map[string]any{"kind": "storage#rewriteResponse", "done": true, "resource": objectJSON(bucket, name, dst)})
}
