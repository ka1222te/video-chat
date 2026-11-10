package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

// /room はルームへの入室と，在室者一覧の通知を行う (WebSocket, JSON)．
//
//	C->S {"type":"join", "roomId", "password", "hostToken"}
//	S->C {"type":"joined", "self":{参加者}, "peers":[参加者...], "token"}   token は 2 人ずつの接続 (/signaling) 用
//	S->C {"type":"peer-joined", "peer":{参加者}} / {"type":"peer-left", "id"} / {"type":"ping"}
//	S->C {"type":"reject", "reason"}  入室できないとき
//	S->C {"type":"closed", "reason"}  ホストがルームを削除したとき
//
// 参加者は {"id", "name", "isHost"}．表示名はルーム内で重ならない動物の名前をサーバが選ぶ．
// 在室人数はこの接続の数で数える (ページを離れると接続が切れて退室になる)．
// 映像・音声は参加者どうしが 2 人ずつ P2P で直接つなぐ (フルメッシュ)．その接続の確立に /signaling を使う．

// animalNames は表示名の候補 (roomCapacity より十分多く用意する)．
var animalNames = []string{
	"パンダ", "キツネ", "ウサギ", "ペンギン", "コアラ", "ラッコ", "フクロウ", "ハリネズミ",
	"カワウソ", "リス", "アザラシ", "ヒツジ", "シロクマ", "レッサーパンダ", "カピバラ", "イルカ",
}

const presencePingInterval = 20 * time.Second // Cloudflare の無通信切断 (約 100 秒) より短くする

type participant struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	IsHost bool   `json:"isHost"`
}

type member struct {
	participant
	token string // この参加者が /signaling で使う秘密の値
	ws    *websocket.Conn
}

type roomHub struct {
	mu    sync.Mutex
	rooms map[string]map[string]*member // ルームID -> 参加者ID -> 在室者
	store *roomStore
}

func newRoomHub(store *roomStore) *roomHub {
	return &roomHub{rooms: map[string]map[string]*member{}, store: store}
}

func (h *roomHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, nil) // 同一オリジンのみ (gate-house は Origin を ORIGIN_URL に書き換えて渡す)
	if err != nil {
		return
	}
	defer ws.CloseNow()
	ws.SetReadLimit(4096)
	ctx := r.Context()
	who := clientLabel(r)

	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, raw, err := ws.Read(readCtx)
	cancel()
	var req struct {
		Type      string `json:"type"`
		RoomID    string `json:"roomId"`
		Password  string `json:"password"`
		HostToken string `json:"hostToken"`
	}
	if err != nil || json.Unmarshal(raw, &req) != nil || req.Type != "join" || len(req.RoomID) > maxIDLen {
		ws.Close(websocket.StatusPolicyViolation, "join required")
		return
	}
	roomID := normalizeRoomID(req.RoomID)

	isHost, err := h.store.enter(roomID, req.Password, req.HostToken)
	if err != nil {
		_, reason := rejectStatus(err)
		log.Printf("room: reject room=%q (%q): %s", roomID, who, reason)
		send(ctx, ws, marshal(map[string]string{"type": "reject", "reason": reason}))
		ws.Close(websocket.StatusNormalClosure, reason)
		return
	}
	self, peers := h.add(roomID, isHost, ws)
	defer h.remove(roomID, self)
	log.Printf("room: join room=%s id=%s name=%s host=%v peers=%d (%q)", roomID, self.ID, self.Name, isHost, len(peers), who)

	joined := map[string]any{"type": "joined", "self": self.participant, "peers": peers, "token": self.token}
	if err := send(ctx, ws, marshal(joined)); err != nil {
		return
	}
	h.broadcast(roomID, self.ID, marshal(map[string]any{"type": "peer-joined", "peer": self.participant}))

	ctx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go func() { // 定期 ping (無通信切断の防止)
		ticker := time.NewTicker(presencePingInterval)
		defer ticker.Stop()
		ping := marshal(map[string]string{"type": "ping"})
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if send(ctx, ws, ping) != nil {
					return
				}
			}
		}
	}()
	for { // クライアントからは何も受け取らない (切断の検出のみ)
		if _, _, err := ws.Read(ctx); err != nil {
			return
		}
	}
}

// add は在室者を追加し，自分の情報とそれ以外の在室者一覧を返す (定員は store.enter で確認済み)．
func (h *roomHub) add(roomID string, isHost bool, ws *websocket.Conn) (*member, []participant) {
	h.mu.Lock()
	defer h.mu.Unlock()
	members := h.rooms[roomID]
	if members == nil {
		members = map[string]*member{}
		h.rooms[roomID] = members
	}
	used := map[string]bool{}
	peers := []participant{}
	for _, m := range members {
		used[m.Name] = true
		peers = append(peers, m.participant)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	var free []string
	for _, n := range animalNames {
		if !used[n] {
			free = append(free, n)
		}
	}
	m := &member{participant: participant{Name: free[randIntn(len(free))], IsHost: isHost}, token: rand.Text(), ws: ws}
	for {
		m.ID = rand.Text()[:8]
		if _, dup := members[m.ID]; !dup {
			break
		}
	}
	members[m.ID] = m
	return m, peers
}

// remove は在室者を外し，他の在室者に通知して，ルームの在室人数を減らす．
func (h *roomHub) remove(roomID string, m *member) {
	h.mu.Lock()
	if members := h.rooms[roomID]; members != nil && members[m.ID] == m {
		delete(members, m.ID)
		if len(members) == 0 {
			delete(h.rooms, roomID)
		}
	}
	h.mu.Unlock()
	h.broadcast(roomID, m.ID, marshal(map[string]string{"type": "peer-left", "id": m.ID}))
	deleted := h.store.exit(roomID)
	log.Printf("room: leave room=%s id=%s name=%s host=%v room_deleted=%v", roomID, m.ID, m.Name, m.IsHost, deleted)
}

// broadcast は except 以外の在室者に送る．
func (h *roomHub) broadcast(roomID, except string, msg []byte) {
	h.mu.Lock()
	var targets []*websocket.Conn
	for id, m := range h.rooms[roomID] {
		if id != except {
			targets = append(targets, m.ws)
		}
	}
	h.mu.Unlock()
	for _, ws := range targets {
		go send(context.Background(), ws, msg) // 遅い相手に引きずられないよう並行して送る
	}
}

// closeRoom はルームの全員に closed を送って切断する (ホストがルームを削除したとき)．
func (h *roomHub) closeRoom(roomID, reason string) {
	h.mu.Lock()
	var targets []*websocket.Conn
	for _, m := range h.rooms[roomID] {
		targets = append(targets, m.ws)
	}
	h.mu.Unlock()
	msg := marshal(map[string]string{"type": "closed", "reason": reason})
	for _, ws := range targets { // 切断の完了 (相手の応答) を待つと API の応答が遅れるため並行して行う
		go func() {
			send(context.Background(), ws, msg)
			ws.Close(websocket.StatusNormalClosure, reason)
		}()
	}
}

// pairRoomID は 2 人の参加者の接続に使うシグナリングのルームID (順序によらず同じになる)．
func pairRoomID(roomID, a, b string) string {
	if a > b {
		a, b = b, a
	}
	return roomID + "/" + a + "-" + b
}

// authorizePair はシグナリングの register を確認する．
// pairID が pairRoomID の形で，2 人とも在室中で，token がそのどちらかのものなら，その参加者を返す．
func (h *roomHub) authorizePair(pairID, token string) (*member, bool) {
	roomID, pair, ok := strings.Cut(pairID, "/")
	if !ok {
		return nil, false
	}
	a, b, ok := strings.Cut(pair, "-")
	if !ok || a == b || a > b {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	members := h.rooms[roomID]
	ma, mb := members[a], members[b]
	if ma == nil || mb == nil {
		return nil, false
	}
	for _, m := range []*member{ma, mb} {
		if subtle.ConstantTimeCompare([]byte(token), []byte(m.token)) == 1 {
			return m, true
		}
	}
	return nil, false
}

// count はルームの在室人数 (テスト用)．
func (h *roomHub) count(roomID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.rooms[roomID])
}
