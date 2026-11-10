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
)

// Ayame (https://github.com/OpenAyame/ayame) 互換のシグナリング．
// go-ayame クライアントが使う register / accept / reject / offer / answer / candidate / ping / pong / bye を扱う．
//
// フルメッシュの各接続 (参加者 2 人の組) ごとに 1 つのシグナリングルームを使う．
//   - roomId は pairRoomID の形 ("<ルームID>/<参加者ID>-<参加者ID>")
//   - signalingKey は /room の joined で受け取った，その参加者のトークン
// 2 人とも /room に在室中で，トークンがどちらかのものである場合だけ入れる．

// pingInterval は go-ayame の受信タイムアウト (90 秒) と Cloudflare の無通信切断 (約 100 秒) より短くする．
const pingInterval = 20 * time.Second

type iceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type sigRegisterMsg struct {
	Type         string  `json:"type"`
	RoomID       string  `json:"roomId"`
	ClientID     string  `json:"clientId"`
	SignalingKey *string `json:"signalingKey"` // /room で受け取った参加者のトークン
}

type sigAcceptMsg struct {
	Type          string      `json:"type"`
	ConnectionID  string      `json:"connectionId"`
	IceServers    []iceServer `json:"iceServers,omitempty"`
	IsExistClient bool        `json:"isExistClient"`
}

type sigRejectMsg struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type sigClient struct {
	ws          *websocket.Conn
	participant string // 参加者ID
}

type signaling struct {
	mu         sync.Mutex
	rooms      map[string][]*sigClient // シグナリングルームID -> 接続中のクライアント (最大 2)
	hub        *roomHub
	iceServers []iceServer
}

func newSignaling(hub *roomHub, ice []iceServer) *signaling {
	return &signaling{rooms: map[string][]*sigClient{}, hub: hub, iceServers: ice}
}

func (s *signaling) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer ws.CloseNow()
	ws.SetReadLimit(1 << 20) // go-ayame 側の上限と同じ 1MB (SDP を含むため)
	ctx := r.Context()
	who := clientLabel(r)

	// 最初のメッセージは register
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, raw, err := ws.Read(readCtx)
	cancel()
	var reg sigRegisterMsg
	if err != nil || json.Unmarshal(raw, &reg) != nil || reg.Type != "register" || reg.RoomID == "" ||
		len(reg.RoomID) > 2*maxIDLen || len(reg.ClientID) > maxIDLen {
		ws.Close(websocket.StatusPolicyViolation, "register required")
		return
	}
	token := ""
	if reg.SignalingKey != nil {
		token = *reg.SignalingKey
	}

	self := &sigClient{ws: ws}
	isExist, reason := s.join(reg.RoomID, token, self)
	if reason != "" {
		log.Printf("signaling: reject room=%q (%q): %s", reg.RoomID, who, reason)
		send(ctx, ws, marshal(sigRejectMsg{Type: "reject", Reason: reason}))
		ws.Close(websocket.StatusNormalClosure, reason)
		return
	}
	defer s.leave(reg.RoomID, self)

	accept := sigAcceptMsg{Type: "accept", ConnectionID: rand.Text(), IceServers: s.iceServers, IsExistClient: isExist}
	if err := send(ctx, ws, marshal(accept)); err != nil {
		return
	}

	ctx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go func() { // 定期 ping (クライアントは pong を返す)
		ticker := time.NewTicker(pingInterval)
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

	for {
		_, raw, err := ws.Read(ctx)
		if err != nil {
			return
		}
		var msg struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &msg) != nil {
			return
		}
		switch msg.Type {
		case "offer", "answer", "candidate": // 相手にそのまま転送
			if peer := s.peer(reg.RoomID, self); peer != nil {
				send(ctx, peer.ws, raw)
			}
		case "pong":
		default:
			log.Printf("signaling: unknown message type %q from %q", msg.Type, who)
		}
	}
}

// join は 2 人の組のシグナリングルームに入る．reason が空でなければ拒否．
func (s *signaling) join(pairID, token string, c *sigClient) (isExist bool, reason string) {
	m, ok := s.hub.authorizePair(pairID, token)
	if !ok {
		return false, "INVALID-PAIR"
	}
	c.participant = m.ID
	s.mu.Lock()
	defer s.mu.Unlock()
	members := s.rooms[pairID]
	for _, other := range members {
		if other.participant == c.participant { // 同じ参加者が同じ組に二重に入ることはできない
			return false, "DUPLICATE"
		}
	}
	if len(members) >= 2 {
		return false, "ROOM-FULL"
	}
	s.rooms[pairID] = append(members, c)
	return len(members) == 1, ""
}

// leave は退室し，残った相手に bye を送る．
func (s *signaling) leave(pairID string, c *sigClient) {
	s.mu.Lock()
	var rest []*sigClient
	for _, m := range s.rooms[pairID] {
		if m != c {
			rest = append(rest, m)
		}
	}
	if len(rest) == 0 {
		delete(s.rooms, pairID)
	} else {
		s.rooms[pairID] = rest
	}
	s.mu.Unlock()
	for _, m := range rest {
		send(context.Background(), m.ws, marshal(map[string]string{"type": "bye"}))
	}
}

func (s *signaling) peer(pairID string, self *sigClient) *sigClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.rooms[pairID] {
		if m != self {
			return m
		}
	}
	return nil
}

func send(ctx context.Context, ws *websocket.Conn, b []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return ws.Write(ctx, websocket.MessageText, b)
}
