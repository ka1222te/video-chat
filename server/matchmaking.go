package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// keepAliveInterval は待機中に送るメッセージの間隔．
// Cloudflare は約 100 秒無通信の WebSocket を切断するため，それより十分短くする．
const keepAliveInterval = 30 * time.Second

// maxIDLen はクライアントが送る ID (ユーザID・クライアントID・ルームID) の最大長．
const maxIDLen = 64

type mmReqMsg struct { // クライアント -> サーバ
	UserID    string    `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
}

type mmResMsg struct { // サーバ -> クライアント (Type: WAITING / MATCH)
	Type      string    `json:"type"`
	RoomID    string    `json:"room_id,omitempty"`
	UserID    string    `json:"user_id,omitempty"` // マッチ相手のユーザID
	CreatedAt time.Time `json:"created_at"`
}

type mmClient struct {
	userID  string
	matched chan mmResMsg // マッチ成立時に 1 回だけ送られる
}

// matchmaker は先着 2 人をペアにしてルームIDを払い出す．
type matchmaker struct {
	mu      sync.Mutex
	waiting *mmClient
	rooms   *roomRegistry
}

func newMatchmaker(rooms *roomRegistry) *matchmaker {
	return &matchmaker{rooms: rooms}
}

func (m *matchmaker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Origin は Host と一致するもののみ許可 (gate-house は Origin を ORIGIN_URL に書き換えて渡す)
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer ws.CloseNow()
	ws.SetReadLimit(4096)
	who := clientLabel(r)

	readCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	var req mmReqMsg
	err = wsjson.Read(readCtx, ws, &req)
	cancel()
	if err != nil || req.UserID == "" || len(req.UserID) > maxIDLen {
		log.Printf("matchmaking: invalid request from %q: %v", who, err)
		ws.Close(websocket.StatusPolicyViolation, "invalid request")
		return
	}

	self := &mmClient{userID: req.UserID, matched: make(chan mmResMsg, 1)}
	m.enqueue(self)
	log.Printf("matchmaking: waiting user=%q (%q)", self.userID, who)

	ctx := ws.CloseRead(r.Context()) // 以降の受信は不要．切断されると ctx が終了する
	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case res := <-self.matched:
			if err := write(ctx, ws, res); err != nil {
				log.Printf("matchmaking: failed to send MATCH to user=%q: %v", self.userID, err)
				return
			}
			log.Printf("matchmaking: matched user=%q room=%s", self.userID, res.RoomID)
			ws.Close(websocket.StatusNormalClosure, "matched")
			return
		case <-ticker.C:
			if err := write(ctx, ws, mmResMsg{Type: "WAITING", CreatedAt: time.Now()}); err != nil {
				m.cancel(self)
				return
			}
		case <-ctx.Done():
			m.cancel(self)
			log.Printf("matchmaking: left user=%q", self.userID)
			return
		}
	}
}

// enqueue は待機中のユーザがいればペアにし，いなければ待機させる．
func (m *matchmaker) enqueue(c *mmClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	peer := m.waiting
	if peer == nil {
		m.waiting = c
		return
	}
	m.waiting = nil
	roomID := rand.Text()
	m.rooms.add(roomID)
	now := time.Now()
	peer.matched <- mmResMsg{Type: "MATCH", RoomID: roomID, UserID: c.userID, CreatedAt: now}
	c.matched <- mmResMsg{Type: "MATCH", RoomID: roomID, UserID: peer.userID, CreatedAt: now}
}

// cancel は切断したユーザを待機から外す．
func (m *matchmaker) cancel(c *mmClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.waiting == c {
		m.waiting = nil
	}
}

func write(ctx context.Context, ws *websocket.Conn, v any) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return wsjson.Write(ctx, ws, v)
}

// roomRegistry はマッチメイキングで払い出したルームIDを有効期限付きで保持する．
// シグナリングはここに登録されたルームだけを受け付ける．
type roomRegistry struct {
	mu    sync.Mutex
	ttl   time.Duration
	rooms map[string]time.Time // ルームID -> 有効期限
}

func newRoomRegistry(ttl time.Duration) *roomRegistry {
	return &roomRegistry{ttl: ttl, rooms: map[string]time.Time{}}
}

func (r *roomRegistry) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for k, exp := range r.rooms { // 期限切れを掃除
		if now.After(exp) {
			delete(r.rooms, k)
		}
	}
	r.rooms[id] = now.Add(r.ttl)
}

func (r *roomRegistry) valid(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	exp, ok := r.rooms[id]
	return ok && time.Now().Before(exp)
}

// marshal は送信用の JSON を作る (固定の構造体のみ渡すためエラーは起きない)．
func marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
