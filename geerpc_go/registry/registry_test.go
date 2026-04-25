package registry

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func fetchServers(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET registry failed: %v", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	return resp.Header.Get(ServersHeader)
}

func fetchServerInfos(t *testing.T, url string) []ServerInfo {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET registry failed: %v", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	var infos []ServerInfo
	raw := resp.Header.Get(ServerInfosHeader)
	if raw == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(raw), &infos); err != nil {
		t.Fatalf("Unmarshal server infos failed: %v", err)
	}
	return infos
}

func TestHeartbeatControllerStopUnregistersServer(t *testing.T) {
	r := New(time.Minute)
	srv := httptest.NewServer(r)
	defer srv.Close()

	addr := "tcp@127.0.0.1:9000"
	controller := Heartbeat(srv.URL, addr, time.Hour)
	t.Cleanup(func() {
		_ = controller.Stop()
	})

	deadline := time.Now().Add(300 * time.Millisecond)
	registered := false
	for time.Now().Before(deadline) {
		if strings.Contains(fetchServers(t, srv.URL), addr) {
			registered = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !registered {
		t.Fatalf("expected %s to be registered before stop", addr)
	}

	if err := controller.Stop(); err != nil {
		t.Fatalf("Stop returned err: %v", err)
	}

	if got := fetchServers(t, srv.URL); strings.Contains(got, addr) {
		t.Fatalf("expected %s to be removed from registry, got %q", addr, got)
	}
}

func TestHeartbeatWithOptionsRegistersTaggedServer(t *testing.T) {
	r := New(time.Minute)
	srv := httptest.NewServer(r)
	defer srv.Close()

	controller := HeartbeatWithOptions(srv.URL, "tcp@127.0.0.1:9001", time.Hour, &ServerOptions{
		Group:   "gray",
		Version: "v2",
		Weight:  3,
	})
	t.Cleanup(func() {
		_ = controller.Stop()
	})

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		infos := fetchServerInfos(t, srv.URL)
		if len(infos) == 1 {
			if infos[0].Group != "gray" || infos[0].Version != "v2" || infos[0].Weight != 3 {
				t.Fatalf("unexpected tagged info: %+v", infos[0])
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected tagged server info to be registered")
}

func TestRegistryDefaultsLegacyServerInfo(t *testing.T) {
	r := New(time.Minute)
	srv := httptest.NewServer(r)
	defer srv.Close()

	req := httptest.NewRequest(http.MethodPost, defaultPath, nil)
	req.Header.Set(ServerHeader, "tcp@127.0.0.1:9002")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	infos := fetchServerInfos(t, srv.URL)
	if len(infos) != 1 {
		t.Fatalf("unexpected infos length: %d", len(infos))
	}
	if infos[0].Group != DefaultGroup || infos[0].Version != DefaultVersion || infos[0].Weight != DefaultWeight {
		t.Fatalf("unexpected default info: %+v", infos[0])
	}
}
