package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	rooms := newRoomRegistry(time.Minute)
	mux := http.NewServeMux()
	mux.Handle("/matchmaking", newMatchmaker(rooms))
	mux.Handle("/signaling", newSignaling(rooms, []iceServer{{URLs: []string{"stun:stun.example:3478"}}}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func dial(t *testing.T, ctx context.Context, srv *httptest.Server, path string) *websocket.Conn {
	t.Helper()
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+path, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	t.Cleanup(func() { ws.CloseNow() })
	return ws
}

// 2 人がマッチし，同じルームでシグナリングのメッセージが相手に届くこと
func TestMatchAndSignal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv := newTestServer(t)

	match := func(user string, out chan<- mmResMsg) {
		ws := dial(t, ctx, srv, "/matchmaking")
		wsjson.Write(ctx, ws, mmReqMsg{UserID: user, CreatedAt: time.Now()})
		var res mmResMsg
		if err := wsjson.Read(ctx, ws, &res); err != nil {
			t.Errorf("read match: %v", err)
		}
		out <- res
	}
	ch := make(chan mmResMsg, 2)
	go match("alice", ch)
	go match("bob", ch)
	a, b := <-ch, <-ch
	if a.Type != "MATCH" || a.RoomID == "" || a.RoomID != b.RoomID {
		t.Fatalf("unexpected match: %+v %+v", a, b)
	}

	register := func(client string) (*websocket.Conn, sigAcceptMsg) {
		ws := dial(t, ctx, srv, "/signaling")
		wsjson.Write(ctx, ws, sigRegisterMsg{Type: "register", RoomID: a.RoomID, ClientID: client})
		var acc sigAcceptMsg
		if err := wsjson.Read(ctx, ws, &acc); err != nil || acc.Type != "accept" {
			t.Fatalf("accept: %+v %v", acc, err)
		}
		return ws, acc
	}
	first, acc1 := register("c1")
	second, acc2 := register("c2")
	if acc1.IsExistClient || !acc2.IsExistClient || len(acc2.IceServers) != 1 {
		t.Fatalf("unexpected accept: %+v %+v", acc1, acc2)
	}

	offer := map[string]string{"type": "offer", "sdp": "v=0"}
	wsjson.Write(ctx, second, offer)
	var got map[string]string
	if err := wsjson.Read(ctx, first, &got); err != nil || got["sdp"] != "v=0" {
		t.Fatalf("relay offer: %+v %v", got, err)
	}

	// 3 人目は満室で拒否
	third := dial(t, ctx, srv, "/signaling")
	wsjson.Write(ctx, third, sigRegisterMsg{Type: "register", RoomID: a.RoomID, ClientID: "c3"})
	var rej sigRejectMsg
	if err := wsjson.Read(ctx, third, &rej); err != nil || rej.Type != "reject" {
		t.Fatalf("expected reject: %+v %v", rej, err)
	}

	// 退室すると相手に bye
	second.Close(websocket.StatusNormalClosure, "")
	if err := wsjson.Read(ctx, first, &got); err != nil || got["type"] != "bye" {
		t.Fatalf("expected bye: %+v %v", got, err)
	}
}

// マッチメイキングで払い出していないルームには入れないこと
func TestSignalingRejectsUnknownRoom(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv := newTestServer(t)
	ws := dial(t, ctx, srv, "/signaling")
	wsjson.Write(ctx, ws, sigRegisterMsg{Type: "register", RoomID: "guess", ClientID: "x"})
	_, raw, err := ws.Read(ctx)
	var rej sigRejectMsg
	if err != nil || json.Unmarshal(raw, &rej) != nil || rej.Reason != "INVALID-ROOM-ID" {
		t.Fatalf("expected INVALID-ROOM-ID: %s %v", raw, err)
	}
}

func TestParseEnv(t *testing.T) {
	env, err := parseEnv([]byte("# c\nA='x # y'  # comment\nexport B=\"2\"\nC=3 # note\nD=\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"A": "x # y", "B": "2", "C": "3", "D": ""}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("%s = %q, want %q", k, env[k], v)
		}
	}
}

// ディレクトリ一覧を返さず，セキュリティヘッダが付くこと
func TestStaticHardening(t *testing.T) {
	dir := t.TempDir()
	os.Mkdir(dir+"/sub", 0o755)
	os.WriteFile(dir+"/sub/secret.txt", []byte("x"), 0o644)
	os.WriteFile(dir+"/index.html", []byte("<html></html>"), 0o644)
	srv := httptest.NewServer(securityHeaders(http.FileServer(noDirListing{http.Dir(dir)})))
	defer srv.Close()

	for path, want := range map[string]int{"/": 200, "/sub/": 404, "/sub/secret.txt": 200} {
		res, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != want {
			t.Errorf("%s: status %d, want %d", path, res.StatusCode, want)
		}
		if !strings.Contains(res.Header.Get("Content-Security-Policy"), "frame-ancestors 'none'") ||
			res.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s: missing security headers: %v", path, res.Header)
		}
	}
}

// 同時接続数の上限を超えると 503 になること
func TestLimitConns(t *testing.T) {
	sem := make(chan struct{}, 1)
	sem <- struct{}{} // 上限まで使用中
	h := limitConns(sem, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rec.Code)
	}
}

// 他サイトのオリジンからの WebSocket は拒否されること
func TestRejectCrossOriginWebSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv := newTestServer(t)
	_, res, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/matchmaking",
		&websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"https://evil.example"}}})
	if err == nil || res == nil || res.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got res=%v err=%v", res, err)
	}
}
