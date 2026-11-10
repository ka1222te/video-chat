//go:build js && wasm
// +build js,wasm

// Command client はブラウザで動く Video Chat の接続処理 (Go WebAssembly)．
//
// 担当は接続の確立だけで，映像・音声の扱いと画面は JS (public/index.html) が行う．
//  1. /room に入室し，在室者一覧と入退室の通知を受け取る (JS の onRoomEvent に渡す)
//  2. 在室者ごとに go-ayame で P2P 接続を確立する (フルメッシュ．2 人の組ごとに /signaling のルームを使う)
//  3. 確立したら RTCPeerConnection と DataChannel を JS の onPeerReady に渡す
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall/js"
	"time"
	"unsafe"

	"github.com/pion/webrtc/v3"
	"nhooyr.io/websocket"
	"video-chat/go-ayame"
)

const (
	p2pConnectTimeout = 60 * time.Second // 相手との P2P 接続の完了を待つ上限
)

func main() {
	// ルーム・シグナリングはページと同じオリジンで提供される．
	// トンネル経由(https)なら wss，localhost 直接(http)なら ws で接続する．
	location := js.Global().Get("location")
	wsScheme := "ws"
	if location.Get("protocol").String() == "https:" {
		wsScheme = "wss"
	}
	host := location.Get("host").String()
	roomURL := url.URL{Scheme: wsScheme, Host: host, Path: "/room"}
	signalingURL := url.URL{Scheme: wsScheme, Host: host, Path: "/signaling"}

	var inProgress atomic.Bool // 入室中フラグ(二重起動防止)

	// NewVideoChat(roomId, password, hostToken) はルームに入り，在室者全員と P2P 接続を確立する．
	// hostToken はルームを作成した人だけが持つ (参加者は空文字)．
	js.Global().Set("NewVideoChat", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 3 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString || args[2].Type() != js.TypeString {
			return js.Undefined()
		}
		if !inProgress.CompareAndSwap(false, true) { // 実行中なら何もしない
			logElem("[Sys]: Already started.")
			return js.Undefined()
		}
		r := &room{
			roomURL:      roomURL.String(),
			signalingURL: signalingURL.String(),
			peers:        map[string]*ayame.Connection{},
		}
		go func() {
			r.run(args[0].String(), args[1].String(), args[2].String())
			inProgress.Store(false)
		}()
		return js.Undefined()
	}))
	select {} // 永続待機
}

type room struct {
	roomURL, signalingURL string
	roomID, selfID, token string // token は 2 人ずつの接続のシグナリングに使う (サーバが発行)

	mu    sync.Mutex
	peers map[string]*ayame.Connection // 相手の参加者ID -> 接続 (確立中・確立済み)
}

// run は /room に入室し，退室するまで在室者の入退室に合わせて接続を張る・外す．
func (r *room) run(roomID, password, hostToken string) {
	ctx := context.Background()
	ws, _, err := websocket.Dial(ctx, r.roomURL, nil)
	if err != nil {
		log.Printf("room: %v", err)
		callStatus("failed", "NETWORK")
		return
	}
	defer ws.CloseNow()
	ws.SetReadLimit(64 << 10)
	join, _ := json.Marshal(map[string]string{"type": "join", "roomId": roomID, "password": password, "hostToken": hostToken})
	if err := ws.Write(ctx, websocket.MessageText, join); err != nil {
		callStatus("failed", "NETWORK")
		return
	}

	joined := false
	defer r.closeAll()
	for {
		_, raw, err := ws.Read(ctx)
		if err != nil { // サーバの停止・ネットワークの切断など
			if joined {
				callStatus("ended", "EXIT-RECV")
			} else {
				callStatus("failed", "NETWORK")
			}
			return
		}
		var msg struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
			Token  string `json:"token"`
			ID     string `json:"id"`
			Self   struct {
				ID string `json:"id"`
			} `json:"self"`
			Peer struct {
				ID string `json:"id"`
			} `json:"peer"`
			Peers []struct {
				ID string `json:"id"`
			} `json:"peers"`
		}
		if json.Unmarshal(raw, &msg) != nil {
			continue
		}
		switch msg.Type {
		case "joined": // 入室できた．今いる全員とつなぐ
			joined = true
			r.roomID, r.selfID, r.token = normalizeRoomID(roomID), msg.Self.ID, msg.Token
			roomEvent(raw)
			for _, p := range msg.Peers {
				r.connect(p.ID)
			}
		case "peer-joined": // 新しく入った人とつなぐ
			roomEvent(raw)
			r.connect(msg.Peer.ID)
		case "peer-left":
			r.disconnect(msg.ID)
			roomEvent(raw)
		case "reject": // 入室できなかった (パスワード違い・満室など)
			callStatus("failed", msg.Reason)
			return
		case "closed": // ホストがルームを削除した
			callStatus("ended", msg.Reason)
			return
		}
	}
}

// connect は相手との P2P 接続を確立する (確立したら JS の onPeerReady に渡す)．
func (r *room) connect(peerID string) {
	if peerID == "" || peerID == r.selfID {
		return
	}
	a, b := r.selfID, peerID
	if a > b {
		a, b = b, a
	}
	pairID := r.roomID + "/" + a + "-" + b // サーバの pairRoomID と同じ形

	opts := ayame.DefaultOptions()                                // デフォルト設定(シグナリングサーバ)
	opts.Audio.Enabled = true                                     // 音声のトランシーバを作る (トラックは JS が設定する)
	opts.Audio.Direction = webrtc.RTPTransceiverDirectionSendrecv // 送受信
	opts.Video.Enabled = true                                     // 映像のトランシーバを作る
	opts.Video.Direction = webrtc.RTPTransceiverDirectionSendrecv // 送受信
	opts.SignalingKey = r.token                                   // この参加者のトークン (register の signalingKey で送る)
	// シグナリングサーバ接続(URL，ルームID，設定，デバッグ用(SDP 等をコンソールに出さない)，websocket自動切断有効可否)
	conn := ayame.NewConnection(r.signalingURL, pairID, opts, false, false)

	r.mu.Lock()
	if old := r.peers[peerID]; old != nil {
		r.mu.Unlock()
		return // 接続中・接続済み
	}
	r.peers[peerID] = conn
	r.mu.Unlock()

	go r.establish(peerID, conn)
}

func (r *room) establish(peerID string, conn *ayame.Connection) {
	connected := make(chan struct{}, 1)          // P2P接続完了(ICE 再接続で複数回通知されても詰まらないようバッファ付き)
	dcReady := make(chan *webrtc.DataChannel, 1) // DataChannel 取得(作成側・受信側のどちらか一方から 1 回)
	failed := make(chan error, 1)                // 拒否・切断など
	var isConnected atomic.Bool
	notify := func(ch chan struct{}) {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	fail := func(err error) {
		select {
		case failed <- err:
		default:
		}
	}
	setDC := func(c *webrtc.DataChannel) {
		select {
		case dcReady <- c:
		default:
		}
	}

	conn.OnOpen(func(metadata *interface{}) { // シグナリングのルームに入れたとき
		// 後から入った側が DataChannel を作って offer を送る (先に入った側は OnDataChannel で受け取る)
		if dc, err := conn.CreateDataChannel("video-chat", nil); err == nil {
			setDC(dc)
		}
	})
	conn.OnDataChannel(setDC)
	conn.OnConnect(func() { // P2Pコネクション接続時
		isConnected.Store(true)
		notify(connected)
	})
	conn.OnDisconnect(func(reason string, err error) { // 拒否・シグナリング切断・ICE 失敗
		if !isConnected.Load() {
			fail(errors.New(reason))
		}
	})
	conn.OnBye(func() { // 接続前に相手がシグナリングから抜けた
		if !isConnected.Load() {
			fail(errors.New("PEER-LEFT"))
		}
	})

	if err := conn.Connect(); err != nil {
		r.peerFailed(peerID, conn, err)
		return
	}
	timeout := time.After(p2pConnectTimeout)
	var dc *webrtc.DataChannel
	for ok := false; !ok || dc == nil; {
		select {
		case <-connected:
			ok = true
		case dc = <-dcReady:
		case err := <-failed:
			r.peerFailed(peerID, conn, err)
			return
		case <-timeout: // NAT 越え失敗などで繋がらないとき
			r.peerFailed(peerID, conn, errors.New("TIMEOUT"))
			return
		}
	}
	if !r.current(peerID, conn) { // 待っている間に相手が退室した
		conn.Disconnect()
		return
	}
	rawDC, err := rawDataChannel(dc)
	pc := conn.PeerConnection()
	if err != nil || pc == nil {
		r.peerFailed(peerID, conn, fmt.Errorf("DATACHANNEL: %v", err))
		return
	}
	conn.CloseWebSocketConnection() // 確立後はシグナリング不要 (在室の管理は /room で行う)
	logElem("[Sys]: Connected to " + peerID + ".")
	js.Global().Call("onPeerReady", peerID, pc.JSValue(), rawDC)
}

// peerFailed は接続できなかった相手を JS に伝える (相手が退室済みなら何もしない)．
func (r *room) peerFailed(peerID string, conn *ayame.Connection, err error) {
	conn.Disconnect()
	if !r.current(peerID, conn) {
		return
	}
	r.mu.Lock()
	delete(r.peers, peerID)
	r.mu.Unlock()
	log.Printf("peer %s: %v", peerID, err)
	logElem(fmt.Sprintf("[Sys]: Could not connect to %s (%v).", peerID, err))
	js.Global().Call("onPeerFailed", peerID, err.Error())
}

func (r *room) current(peerID string, conn *ayame.Connection) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peers[peerID] == conn
}

// disconnect は退室した相手との接続を閉じる．
func (r *room) disconnect(peerID string) {
	r.mu.Lock()
	conn := r.peers[peerID]
	delete(r.peers, peerID)
	r.mu.Unlock()
	if conn != nil {
		conn.Disconnect()
	}
}

func (r *room) closeAll() {
	r.mu.Lock()
	peers := r.peers
	r.peers = map[string]*ayame.Connection{}
	r.mu.Unlock()
	for _, conn := range peers {
		conn.Disconnect()
	}
}

// normalizeRoomID はサーバと同じ規則でルームIDを整える (大文字化・空白とハイフンの除去)．
func normalizeRoomID(id string) string {
	return strings.Map(func(r rune) rune {
		if r == ' ' || r == '-' {
			return -1
		}
		return r
	}, strings.ToUpper(strings.TrimSpace(id)))
}

// rawDataChannel は pion/webrtc (js/wasm) の DataChannel が内部に持つブラウザの RTCDataChannel を取り出す．
// pion は公開 API を用意していないため，構造体の先頭フィールド (underlying js.Value) を読む．
// go.mod で固定している pion/webrtc v3.3.6 の datachannel_js.go の定義に依存するため，更新時は要確認．
// 取り出した値が想定と異なる場合 (label の不一致など) はエラーを返す．
func rawDataChannel(dc *webrtc.DataChannel) (js.Value, error) {
	v := *(*js.Value)(unsafe.Pointer(dc))
	if v.Type() != js.TypeObject || v.Get("send").Type() != js.TypeFunction || v.Get("label").String() != dc.Label() {
		return js.Undefined(), fmt.Errorf("unexpected pion/webrtc DataChannel layout")
	}
	v.Set("onmessage", js.Null()) // go-ayame が設定した受信処理(受信データを毎回 Go にコピーする)を外す
	return v, nil
}

// roomEvent は /room から届いたメッセージ (joined / peer-joined / peer-left) を JS に渡す．
func roomEvent(raw []byte) {
	js.Global().Call("onRoomEvent", string(raw))
}

func logElem(msg string) { // ログ表示 (JS の appendLog は textarea の value に追記するため HTML として解釈されない)
	js.Global().Call("appendLog", msg)
}

// callStatus はルーム全体の状態 (failed / ended) と理由を画面に伝える．
func callStatus(status, reason string) {
	if f := js.Global().Get("onCallStatus"); f.Type() == js.TypeFunction {
		f.Invoke(status, reason)
	}
}
