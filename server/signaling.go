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

// pingInterval は go-ayame の受信タイムアウト (90 秒) と Cloudflare の無通信切断 (約 100 秒) より短くする．
const pingInterval = 20 * time.Second

type iceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type sigRegisterMsg struct {
	Type     string `json:"type"`
	RoomID   string `json:"roomId"`
	ClientID string `json:"clientId"`
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
	ws       *websocket.Conn
	clientID string
}

type signaling struct {
	mu         sync.Mutex
	rooms      map[string][]*sigClient // ルームID -> 入室中のクライアント (最大 2)
	allowed    *roomRegistry
	iceServers []iceServer
}

func newSignaling(allowed *roomRegistry, ice []iceServer) *signaling {
	return &signaling{rooms: map[string][]*sigClient{}, allowed: allowed, iceServers: ice}
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
		len(reg.RoomID) > maxIDLen || len(reg.ClientID) > maxIDLen {
		ws.Close(websocket.StatusPolicyViolation, "register required")
		return
	}

	self := &sigClient{ws: ws, clientID: reg.ClientID}
	isExist, reason := s.join(reg.RoomID, self)
	if reason != "" {
		log.Printf("signaling: reject room=%q (%q): %s", reg.RoomID, who, reason)
		send(ctx, ws, marshal(sigRejectMsg{Type: "reject", Reason: reason}))
		ws.Close(websocket.StatusNormalClosure, reason)
		return
	}
	defer s.leave(reg.RoomID, self)
	log.Printf("signaling: join room=%s client=%q exist=%v (%q)", reg.RoomID, reg.ClientID, isExist, who)

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

// join はルームに入室する．reason が空でなければ拒否．
func (s *signaling) join(roomID string, c *sigClient) (isExist bool, reason string) {
	if !s.allowed.valid(roomID) {
		return false, "INVALID-ROOM-ID"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	members := s.rooms[roomID]
	if len(members) >= 2 {
		return false, "full"
	}
	s.rooms[roomID] = append(members, c)
	return len(members) == 1, ""
}

// leave は退室し，残った相手に bye を送る．
func (s *signaling) leave(roomID string, c *sigClient) {
	s.mu.Lock()
	var rest []*sigClient
	for _, m := range s.rooms[roomID] {
		if m != c {
			rest = append(rest, m)
		}
	}
	if len(rest) == 0 {
		delete(s.rooms, roomID)
	} else {
		s.rooms[roomID] = rest
	}
	s.mu.Unlock()
	for _, m := range rest {
		send(context.Background(), m.ws, marshal(map[string]string{"type": "bye"}))
	}
	log.Printf("signaling: leave room=%s client=%q", roomID, c.clientID)
}

func (s *signaling) peer(roomID string, self *sigClient) *sigClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.rooms[roomID] {
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
