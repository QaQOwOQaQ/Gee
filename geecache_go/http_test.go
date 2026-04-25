package geecache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	pb "geecache/geecachepb"

	"google.golang.org/protobuf/proto"
)

var testGroupSeq uint64

func uniqueGroupName(prefix string) string {
	id := atomic.AddUint64(&testGroupSeq, 1)
	return fmt.Sprintf("%s_%d", prefix, id)
}

func TestHTTPPoolPickPeerBeforeSetDoesNotPanic(t *testing.T) {
	pool := NewHTTPPool("http://self")
	peer, ok := pool.PickPeer("k")
	if ok || peer != nil {
		t.Fatalf("expected no peer before Set, got ok=%v peer=%v", ok, peer)
	}
}

func TestHTTPQueryRoundTripSpecialChars(t *testing.T) {
	groupName := uniqueGroupName("http_roundtrip")
	var gotKey string
	g := NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		gotKey = key
		return []byte("value:" + key), nil
	}), 0, 0)
	_ = g

	pool := NewHTTPPool("http://self")
	ts := httptest.NewServer(pool)
	defer ts.Close()

	getter := &httpGetter{baseURL: ts.URL + defaultBasePath, client: ts.Client()}
	cases := []string{
		"a b",
		"a+b",
		"a/b",
		"中文/空 格+plus",
	}
	for _, key := range cases {
		res := &pb.Response{}
		err := getter.Get(&pb.Request{Group: groupName, Key: key}, res)
		if err != nil {
			t.Fatalf("getter.Get failed for key %q: %v", key, err)
		}
		if gotKey != key {
			t.Fatalf("key round-trip mismatch: want %q got %q", key, gotKey)
		}
		want := "value:" + key
		if string(res.Value) != want {
			t.Fatalf("response mismatch: want %q got %q", want, string(res.Value))
		}
	}
}

func TestServeHTTPMethodValidation(t *testing.T) {
	groupName := uniqueGroupName("http_method")
	NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return []byte("ok"), nil
	}), 0, 0)

	pool := NewHTTPPool("http://self")
	ts := httptest.NewServer(pool)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+defaultBasePath+"?group="+groupName+"&key=k", nil)
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestServeHTTPDeleteInvalidatesOwnerCache(t *testing.T) {
	groupName := uniqueGroupName("http_delete")
	var loads int32
	NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		atomic.AddInt32(&loads, 1)
		return []byte("v"), nil
	}), 0, 0)

	pool := NewHTTPPool("http://self")
	ts := httptest.NewServer(pool)
	defer ts.Close()
	getter := &httpGetter{baseURL: ts.URL + defaultBasePath, client: ts.Client()}

	res := &pb.Response{}
	if err := getter.Get(&pb.Request{Group: groupName, Key: "k"}, res); err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if atomic.LoadInt32(&loads) != 1 {
		t.Fatalf("expected 1 load, got %d", loads)
	}
	if err := getter.Delete(&pb.Request{Group: groupName, Key: "k"}); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if err := getter.Get(&pb.Request{Group: groupName, Key: "k"}, res); err != nil {
		t.Fatalf("second get failed: %v", err)
	}
	if atomic.LoadInt32(&loads) != 2 {
		t.Fatalf("expected reload after delete, got loads=%d", loads)
	}
}

func TestOwnerForwardingAcrossNodes(t *testing.T) {
	groupName := uniqueGroupName("owner_forward")
	ownerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if got := r.URL.Query().Get("group"); got != groupName {
			t.Fatalf("unexpected group %q", got)
		}
		body, err := proto.Marshal(&pb.Response{Value: []byte("owner:" + r.URL.Query().Get("key"))})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer ownerServer.Close()

	g := NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		return nil, fmt.Errorf("local getter should not be called")
	}), 0, 0)
	g.RegisterPeers(staticPeerPicker{
		peer: &httpGetter{baseURL: ownerServer.URL + defaultBasePath, client: ownerServer.Client()},
		ok:   true,
	})

	v, err := g.Get("Tom")
	if err != nil {
		t.Fatalf("group get failed: %v", err)
	}
	if got, want := v.String(), "owner:Tom"; got != want {
		t.Fatalf("want %q got %q", want, got)
	}
}

func TestRemoteDeletePreventsOwnerStaleBackfill(t *testing.T) {
	groupName := uniqueGroupName("remote_delete_race")
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	var calls int32
	var mu sync.Mutex
	store := "old"

	pool := NewHTTPPool("http://self")
	server := httptest.NewServer(pool)
	defer server.Close()

	NewGroup(groupName, 1<<20, GetterFunc(func(key string) ([]byte, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			loadStarted <- struct{}{}
			<-releaseLoad
		}
		mu.Lock()
		defer mu.Unlock()
		return []byte(store), nil
	}), 0, 0)

	getter := &httpGetter{baseURL: server.URL + defaultBasePath, client: server.Client()}

	done := make(chan struct{})
	go func() {
		res := &pb.Response{}
		_ = getter.Get(&pb.Request{Group: groupName, Key: "Tom"}, res)
		close(done)
	}()

	<-loadStarted
	mu.Lock()
	store = "new"
	mu.Unlock()
	if err := getter.Delete(&pb.Request{Group: groupName, Key: "Tom"}); err != nil {
		t.Fatalf("remote delete failed: %v", err)
	}
	close(releaseLoad)
	<-done

	res := &pb.Response{}
	if err := getter.Get(&pb.Request{Group: groupName, Key: "Tom"}, res); err != nil {
		t.Fatalf("final get failed: %v", err)
	}
	if got := string(res.Value); got != "new" {
		t.Fatalf("stale backfill detected, want %q got %q", "new", got)
	}
}

type staticPeerPicker struct {
	peer PeerGetter
	ok   bool
}

func (p staticPeerPicker) PickPeer(key string) (PeerGetter, bool) {
	return p.peer, p.ok
}
