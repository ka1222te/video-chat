//go:build js && wasm
// +build js,wasm

package main

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync/atomic"
	"syscall/js"
	"time"
	"unsafe"

	"github.com/pion/webrtc/v3"
	"video-chat/go-ayame"
)

const (
	p2pConnectTimeout = 60 * time.Second // 相手がいるルームに入ってから P2P 接続の完了を待つ上限
)

func main() {
	// シグナリングはページと同じオリジンで提供される．
	// トンネル経由(https)なら wss，localhost 直接(http)なら ws で接続する．
	location := js.Global().Get("location")
	wsScheme := "ws"
	if location.Get("protocol").String() == "https:" {
		wsScheme = "wss"
	}
	signalingURL := url.URL{Scheme: wsScheme, Host: location.Get("host").String(), Path: "/signaling"} // シグナリングサーバ用URL

	var inProgress atomic.Bool // 入室〜接続処理の実行中フラグ(二重起動防止)

	// NewVideoChat(roomId, password, hostToken) はルームに入り，相手との P2P 接続を確立する．
	// 先に入った側は相手が来るまで待ち，後から入った側が DataChannel を作って接続を始める．
	// hostToken はルームを作成した人だけが持つ (参加者は空文字)．
	js.Global().Set("NewVideoChat", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 3 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString || args[2].Type() != js.TypeString {
			return js.Undefined()
		}
		roomID, password, hostToken := args[0].String(), args[1].String(), args[2].String()
		if !inProgress.CompareAndSwap(false, true) { // 実行中・接続済みなら何もしない
			logElem("[Sys]: Already started.\n")
			return js.Undefined()
		}
		go joinRoom(signalingURL.String(), roomID, password, hostToken, &inProgress)
		return js.Undefined() // 処理が終了しないよう未定義を返す
	}))
	select {} // 永続待機
}

func joinRoom(signalingURL, roomID, password, hostToken string, inProgress *atomic.Bool) {
	connected := make(chan struct{}, 1)          // P2P接続完了(ICE 再接続で複数回通知されても詰まらないようバッファ付き)
	dcReady := make(chan *webrtc.DataChannel, 1) // DataChannel 取得(作成側・受信側のどちらか一方から 1 回)
	peerFound := make(chan struct{}, 1)          // ルームに相手がいた(自分が後から入った)
	failed := make(chan error, 1)                // 拒否・切断など
	notify := func(ch chan struct{}) {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	setDC := func(c *webrtc.DataChannel) {
		c.OnClose(handleError) // 切断時
		select {
		case dcReady <- c:
		default:
		}
	}
	var isConnected atomic.Bool

	opts := ayame.DefaultOptions()                                // デフォルト設定(シグナリングサーバ)
	opts.Audio.Enabled = true                                     // 音声有効
	opts.Audio.Direction = webrtc.RTPTransceiverDirectionSendrecv // 送受信有効
	opts.Video.Enabled = true                                     // 映像有効
	opts.Video.Direction = webrtc.RTPTransceiverDirectionSendrecv // 送受信有効
	opts.SignalingKey = password                                  // ルームのパスワード (register の signalingKey で送る)
	// シグナリングサーバ接続(URL，ルームID，設定，デバッグ用(SDP 等をコンソールに出さない)，websocket自動切断有効可否)
	conn := ayame.NewConnection(signalingURL, roomID, opts, false, false)
	if hostToken != "" { // ホストであることをサーバに伝える (register の authnMetadata)
		var meta interface{} = map[string]string{"hostToken": hostToken}
		conn.AuthnMetadata = &meta
	}

	conn.OnOpen(func(metadata *interface{}) { // ルームに入れたとき
		// 後から入室した側が DataChannel を作る(先に入室した側は OnDataChannel で受け取る)
		dc, err := conn.CreateDataChannel("video-chat", nil)
		if err != nil { // 自分が先に入った → 相手を待つ
			logElem("[Sys]: Joined the room. Waiting for the other person...\n")
			callStatus("waiting", "")
			return
		}
		logElem("[Sys]: Joined the room. Connecting...\n")
		callStatus("connecting", "")
		notify(peerFound)
		setDC(dc)
	})
	conn.OnDataChannel(func(c *webrtc.DataChannel) { // 相手が作った DataChannel を受け取ったとき
		callStatus("connecting", "")
		setDC(c)
	})
	conn.OnConnect(func() { // P2Pコネクション接続時
		isConnected.Store(true)
		// 在室人数をサーバで管理するため，通話中もシグナリングの接続は閉じない (ページを離れると切れる)
		logElem("[Sys]: Connected! Start P2P video chat via DataChannel\n") // ログ
		notify(connected)
	})
	conn.OnDisconnect(func(reason string, err error) { // 拒否・シグナリング切断・ICE 失敗
		if isConnected.Load() { // 通話中
			if reason != "EXIT-RECV" { // ホストによるルームの削除など (サーバの停止だけなら P2P の通話は続く)
				logElem(fmt.Sprintf("[Sys]: Disconnected from the room (%s).\n", reason))
				callStatus("ended", reason)
			}
			return
		}
		select {
		case failed <- fmt.Errorf("%s", reason):
		default:
		}
	})
	conn.OnBye(func() { // 接続前に相手が退出した
		if !isConnected.Load() {
			select {
			case failed <- errors.New("PEER-LEFT"):
			default:
			}
		}
	})

	fail := func(stage string, err error) { // ログを出して再試行できるようにする
		conn.Disconnect()
		log.Printf("%s: %v", stage, err)
		logElem(fmt.Sprintf("[Sys]: %s failed (%v).\n", stage, err))
		inProgress.Store(false)
		callStatus("failed", err.Error())
	}

	if err := conn.Connect(); err != nil { // シグナリング開始
		fail("Signaling", err)
		return
	}

	// P2P 接続と DataChannel の両方がそろうのを待つ．
	// 先に入った側は相手が来るまで待ち続け，相手がいた場合だけ時間制限を設ける．
	var timeout <-chan time.Time
	var dc *webrtc.DataChannel
	for ok := false; !ok || dc == nil; {
		select {
		case <-connected:
			ok = true
		case dc = <-dcReady:
		case <-peerFound:
			timeout = time.After(p2pConnectTimeout)
		case err := <-failed:
			fail("Connection", err)
			return
		case <-timeout: // NAT 越え失敗などで繋がらないとき
			fail("P2P connection", errors.New("TIMEOUT"))
			return
		}
	}
	raw, err := rawDataChannel(dc)
	if err != nil {
		fail("DataChannel", err)
		return
	}
	// 映像・音声の送受信は JS 側で RTCDataChannel を直接使う．
	// Go(WASM) を経由すると 1 フレームごとに大きなデータのコピー・変換がメインスレッドで走り，
	// 特に WASM の最適化コンパイルが終わるまでの間に処理が追いつかず映像が固まるため．
	js.Global().Call("onP2PReady", raw) // JS側に接続完了を通知
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

func logElem(msg string) { // ログ表示
	// JS 側の appendLog と同じく textarea の value に追記する (innerHTML は使わない)
	js.Global().Call("appendLog", strings.TrimSuffix(msg, "\n"))
}

// callStatus は通話の状態 (waiting / connecting / failed / ended) と理由を画面に伝える．
func callStatus(status, reason string) {
	if f := js.Global().Get("onCallStatus"); f.Type() == js.TypeFunction {
		f.Invoke(status, reason)
	}
}

func handleError() { // DataChannel が閉じたとき
	logElem("[Sys]: The other person left or the connection was lost.\n") // ログ
}
