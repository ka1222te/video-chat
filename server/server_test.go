package main

import (
	"bytes"
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

func newTestServer(t *testing.T) (*httptest.Server, *roomStore) {
	t.Helper()
	store := newRoomStore()
	hub := newRoomHub(store)
	api := &roomAPI{store: store, hub: hub}
	mux := http.NewServeMux()
	mux.Handle("/api/rooms", api)
	mux.Handle("/api/rooms/", api) // verify / leave / delete
	mux.Handle("/room", hub)
	mux.Handle("/signaling", newSignaling(hub, []iceServer{{URLs: []string{"stun:stun.example:3478"}}}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, store
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

func postJSON(t *testing.T, srv *httptest.Server, path string, body any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	res, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

type testRoom struct{ id, password, hostToken string }

func createRoom(t *testing.T, srv *httptest.Server) testRoom {
	t.Helper()
	code, res := postJSON(t, srv, "/api/rooms", map[string]string{})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %v", code, res)
	}
	return testRoom{res["roomId"].(string), res["password"].(string), res["hostToken"].(string)}
}

// joinRoom は /room に入室し，最初の応答 (joined または reject) を返す．
func joinRoom(t *testing.T, ctx context.Context, srv *httptest.Server, roomID, password, hostToken string) (*websocket.Conn, map[string]any) {
	t.Helper()
	ws := dial(t, ctx, srv, "/room")
	wsjson.Write(ctx, ws, map[string]string{"type": "join", "roomId": roomID, "password": password, "hostToken": hostToken})
	var res map[string]any
	if err := wsjson.Read(ctx, ws, &res); err != nil {
		t.Fatalf("join: %v", err)
	}
	return ws, res
}

// readType は指定した type のメッセージが届くまで読む (ping などは読み飛ばす)．
func readType(t *testing.T, ctx context.Context, ws *websocket.Conn, typ string) map[string]any {
	t.Helper()
	for {
		var msg map[string]any
		if err := wsjson.Read(ctx, ws, &msg); err != nil {
			t.Fatalf("waiting for %s: %v", typ, err)
		}
		if msg["type"] == typ {
			return msg
		}
	}
}

func selfOf(joined map[string]any) (id, name, token string) {
	self := joined["self"].(map[string]any)
	return self["id"].(string), self["name"].(string), joined["token"].(string)
}

func register(t *testing.T, ctx context.Context, srv *httptest.Server, pairID, token string) (*websocket.Conn, map[string]any) {
	t.Helper()
	ws := dial(t, ctx, srv, "/signaling")
	wsjson.Write(ctx, ws, sigRegisterMsg{Type: "register", RoomID: pairID, ClientID: "c", SignalingKey: &token})
	var res map[string]any
	if err := wsjson.Read(ctx, ws, &res); err != nil {
		t.Fatalf("register: %v", err)
	}
	return ws, res
}

// サーバ側で退室が処理されるまで待つ
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 200 && !cond(); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("timeout")
	}
}

func roomExists(store *roomStore, id string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, ok := store.rooms[id]
	return ok
}

func occupants(store *roomStore, id string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	if r := store.rooms[id]; r != nil {
		return r.occupants
	}
	return -1
}

// 4 人まで入室でき，在室者一覧・入退室の通知・重ならない表示名が届くこと
func TestRoomPresence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv, store := newTestServer(t)
	room := createRoom(t, srv)

	host, joined := joinRoom(t, ctx, srv, room.id, room.password, room.hostToken)
	if joined["type"] != "joined" || joined["self"].(map[string]any)["isHost"] != true || len(joined["peers"].([]any)) != 0 {
		t.Fatalf("host joined: %v", joined)
	}
	hostID, hostName, _ := selfOf(joined)

	names := map[string]bool{hostName: true}
	conns := []*websocket.Conn{host}
	for i := 1; i < roomCapacity; i++ {
		ws, j := joinRoom(t, ctx, srv, strings.ToLower(room.id), room.password, "") // ルームIDは小文字でもよい
		if j["type"] != "joined" || j["self"].(map[string]any)["isHost"] != false || len(j["peers"].([]any)) != i {
			t.Fatalf("guest %d joined: %v", i, j)
		}
		_, name, _ := selfOf(j)
		if names[name] {
			t.Fatalf("duplicate name %q", name)
		}
		names[name] = true
		// 既にいる全員に peer-joined が届く
		for _, c := range conns {
			if p := readType(t, ctx, c, "peer-joined")["peer"].(map[string]any); p["name"] != name {
				t.Fatalf("peer-joined: %v", p)
			}
		}
		conns = append(conns, ws)
	}
	for n := range names {
		found := false
		for _, a := range animalNames {
			found = found || a == n
		}
		if !found {
			t.Errorf("name %q is not an animal name", n)
		}
	}
	if occupants(store, room.id) != roomCapacity {
		t.Fatalf("occupants: %d", occupants(store, room.id))
	}

	// 5 人目は満室 (参加前の確認 API も)
	if _, rej := joinRoom(t, ctx, srv, room.id, room.password, ""); rej["type"] != "reject" || rej["reason"] != "ROOM-FULL" {
		t.Fatalf("expected ROOM-FULL: %v", rej)
	}
	if code, _ := postJSON(t, srv, "/api/rooms/verify", map[string]string{"roomId": room.id, "password": room.password}); code != http.StatusConflict {
		t.Fatalf("verify full: %d", code)
	}

	// ホストが切断すると他の全員に peer-left
	host.Close(websocket.StatusNormalClosure, "")
	for _, c := range conns[1:] {
		if left := readType(t, ctx, c, "peer-left"); left["id"] != hostID {
			t.Fatalf("peer-left: %v", left)
		}
	}
	waitFor(t, func() bool { return occupants(store, room.id) == roomCapacity-1 })
}

// 2 人の組のシグナリングには，その 2 人だけが入れること
func TestPairSignaling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv, _ := newTestServer(t)
	room := createRoom(t, srv)
	_, ja := joinRoom(t, ctx, srv, room.id, room.password, room.hostToken)
	_, jb := joinRoom(t, ctx, srv, room.id, room.password, "")
	_, jc := joinRoom(t, ctx, srv, room.id, room.password, "")
	a, _, tokenA := selfOf(ja)
	b, _, tokenB := selfOf(jb)
	_, _, tokenC := selfOf(jc)
	pair := pairRoomID(room.id, a, b)
	if pair != pairRoomID(room.id, b, a) {
		t.Fatal("pairRoomID must not depend on order")
	}

	// 組に含まれない参加者のトークン・不正な形式は拒否
	for _, tc := range []struct{ pair, token string }{
		{pair, tokenC},
		{pair, "wrong"},
		{room.id, tokenA},
		{room.id + "/" + a + "-" + a, tokenA},
	} {
		if _, rej := register(t, ctx, srv, tc.pair, tc.token); rej["type"] != "reject" || rej["reason"] != "INVALID-PAIR" {
			t.Errorf("%s: expected INVALID-PAIR, got %v", tc.pair, rej)
		}
	}

	wsA, accA := register(t, ctx, srv, pair, tokenA)
	if _, dup := register(t, ctx, srv, pair, tokenA); dup["reason"] != "DUPLICATE" {
		t.Fatalf("expected DUPLICATE: %v", dup)
	}
	wsB, accB := register(t, ctx, srv, pair, tokenB)
	if accA["type"] != "accept" || accA["isExistClient"] != false || accB["type"] != "accept" || accB["isExistClient"] != true {
		t.Fatalf("accept: %v %v", accA, accB)
	}
	if ice, _ := accB["iceServers"].([]any); len(ice) != 1 {
		t.Fatalf("iceServers: %v", accB)
	}
	wsjson.Write(ctx, wsB, map[string]string{"type": "offer", "sdp": "v=0"})
	if got := readType(t, ctx, wsA, "offer"); got["sdp"] != "v=0" {
		t.Fatalf("relay offer: %v", got)
	}
	wsB.Close(websocket.StatusNormalClosure, "")
	readType(t, ctx, wsA, "bye")
}

// 存在しないルーム・誤ったパスワードでは入れないこと
func TestRoomRejects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv, store := newTestServer(t)
	id, _, _, err := store.create("secret-pass")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ room, pw, want string }{
		{"GUESSGUESS", "secret-pass", "INVALID-ROOM-ID"},
		{id, "wrong", "INVALID-PASSWORD"},
		{id, "", "INVALID-PASSWORD"},
	} {
		if _, rej := joinRoom(t, ctx, srv, tc.room, tc.pw, ""); rej["type"] != "reject" || rej["reason"] != tc.want {
			t.Errorf("%s/%q: got %v, want %s", tc.room, tc.pw, rej, tc.want)
		}
	}
	if _, j := joinRoom(t, ctx, srv, id, "secret-pass", "fake-host-token"); j["type"] != "joined" || j["self"].(map[string]any)["isHost"] != false {
		t.Errorf("correct password: %v", j)
	}
}

// ホストが退出しても参加者がいればルームは残り，誰もいなくなったら削除されること
func TestRoomClosesWhenHostLeftAndEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv, store := newTestServer(t)
	room := createRoom(t, srv)

	host, _ := joinRoom(t, ctx, srv, room.id, room.password, room.hostToken)
	guest, _ := joinRoom(t, ctx, srv, room.id, room.password, "")

	// ホストが退出ボタンを押さずに切断しただけ (再読み込みなど) ではルームは残る
	host.Close(websocket.StatusNormalClosure, "")
	waitFor(t, func() bool { return occupants(store, room.id) == 1 })
	host, j := joinRoom(t, ctx, srv, room.id, room.password, room.hostToken) // 入り直してもホストのまま
	if j["self"].(map[string]any)["isHost"] != true {
		t.Fatalf("rejoin as host: %v", j)
	}

	// 参加者はホストになれない
	if code, _ := postJSON(t, srv, "/api/rooms/leave", map[string]string{"roomId": room.id, "hostToken": "fake"}); code != http.StatusForbidden {
		t.Fatalf("leave with wrong token: %d", code)
	}

	// ホストが退出 → 参加者が残っているのでルームは残る
	if code, res := postJSON(t, srv, "/api/rooms/leave", map[string]string{"roomId": room.id, "hostToken": room.hostToken}); code != http.StatusOK || res["deleted"] != false {
		t.Fatalf("leave: %d %v", code, res)
	}
	host.Close(websocket.StatusNormalClosure, "")
	waitFor(t, func() bool { return occupants(store, room.id) == 1 })
	if !roomExists(store, room.id) {
		t.Fatal("room deleted while guest is still in it")
	}

	// 参加者も退出 → 0 人になったので削除
	guest.Close(websocket.StatusNormalClosure, "")
	waitFor(t, func() bool { return !roomExists(store, room.id) })
}

// ホストだけがいるルームで退出するとすぐに削除されること
func TestRoomClosesWhenHostLeavesAlone(t *testing.T) {
	srv, store := newTestServer(t)
	room := createRoom(t, srv)
	if code, res := postJSON(t, srv, "/api/rooms/leave", map[string]string{"roomId": room.id, "hostToken": room.hostToken}); code != http.StatusOK || res["deleted"] != true {
		t.Fatalf("leave: %d %v", code, res)
	}
	if roomExists(store, room.id) {
		t.Fatal("room still exists")
	}
}

// ホストがルームを削除すると在室中の参加者に closed (ROOM-DELETED) が届くこと
func TestHostDeletesRoom(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv, store := newTestServer(t)
	room := createRoom(t, srv)
	joinRoom(t, ctx, srv, room.id, room.password, room.hostToken)
	guest, _ := joinRoom(t, ctx, srv, room.id, room.password, "")

	if code, _ := postJSON(t, srv, "/api/rooms/delete", map[string]string{"roomId": room.id, "hostToken": "fake"}); code != http.StatusForbidden {
		t.Fatalf("delete with wrong token: %d", code)
	}
	if code, _ := postJSON(t, srv, "/api/rooms/delete", map[string]string{"roomId": room.id, "hostToken": room.hostToken}); code != http.StatusOK {
		t.Fatalf("delete: %d", code)
	}
	if msg := readType(t, ctx, guest, "closed"); msg["reason"] != "ROOM-DELETED" {
		t.Fatalf("closed: %v", msg)
	}
	if roomExists(store, room.id) {
		t.Fatal("room still exists")
	}
	if _, rej := joinRoom(t, ctx, srv, room.id, room.password, ""); rej["reason"] != "INVALID-ROOM-ID" {
		t.Fatalf("join deleted room: %v", rej)
	}
}

// パスワードを続けて間違えるとロックされ，期間が過ぎると解除されること
func TestRoomLockout(t *testing.T) {
	store := newRoomStore()
	now := time.Now()
	store.now = func() time.Time { return now }
	id, pw, _, _ := store.create("")
	for i := 0; i < maxFailures; i++ {
		if err := store.verify(id, "wrong"); err != errWrongPassword {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if err := store.verify(id, pw); err != errRoomLocked {
		t.Fatalf("expected lock, got %v", err)
	}
	now = now.Add(failureWindow + time.Second)
	if err := store.verify(id, pw); err != nil {
		t.Fatalf("expected unlock, got %v", err)
	}
	// 誰もいない状態で長く経ったルームは消える
	now = now.Add(roomIdleTTL + time.Second)
	if err := store.verify(id, pw); err != errRoomNotFound {
		t.Fatalf("expected expiry, got %v", err)
	}
}

// gate-house 経由 (Host は中継先，Origin は公開 URL，X-Forwarded-Host は公開 URL のホスト) でも作成できること
func TestRoomAPIThroughTunnel(t *testing.T) {
	srv, _ := newTestServer(t)
	do := func(origin, fwdHost string) int {
		req, _ := http.NewRequest("POST", srv.URL+"/api/rooms", strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", origin)
		if fwdHost != "" {
			req.Header.Set("X-Forwarded-Host", fwdHost)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	if code := do("https://video-chat.example.workers.dev", "video-chat.example.workers.dev"); code != http.StatusCreated {
		t.Errorf("through tunnel: %d", code)
	}
	if code := do("https://evil.example", "video-chat.example.workers.dev"); code != http.StatusForbidden {
		t.Errorf("other site through tunnel: %d", code)
	}
}

// 指定パスワードの長さ制限と，他サイトからの API 呼び出しの拒否
func TestRoomAPIValidation(t *testing.T) {
	srv, _ := newTestServer(t)
	if code, _ := postJSON(t, srv, "/api/rooms", map[string]string{"password": "abc"}); code != http.StatusBadRequest {
		t.Errorf("short password: %d", code)
	}
	if code, res := postJSON(t, srv, "/api/rooms", map[string]string{"password": "my pass"}); code != http.StatusCreated || res["password"] != "my pass" {
		t.Errorf("custom password: %d %v", code, res)
	}
	req, _ := http.NewRequest("POST", srv.URL+"/api/rooms", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://evil.example")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Errorf("cross-origin: %d", res.StatusCode)
	}
	res, err = http.Post(srv.URL+"/api/rooms", "text/plain", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Errorf("non-JSON: %d", res.StatusCode)
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
	srv, _ := newTestServer(t)
	_, res, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/signaling",
		&websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"https://evil.example"}}})
	if err == nil || res == nil || res.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got res=%v err=%v", res, err)
	}
}
