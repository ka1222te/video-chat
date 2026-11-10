//go:build js && wasm
// +build js,wasm

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

const (
	p2pConnectTimeout = 60 * time.Second // マッチング後，P2P 接続の完了を待つ上限
)

type mmReqMsg struct { // リクエストメッセージ(マッチメイキング用)
	UserID    string    `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
}

type mmResMsg struct { // レスポンスメッセージ(マッチメイキング用)
	Type      string    `json:"type"`
	RoomID    string    `json:"room_id"`
	UserID    string    `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	// マッチメイキング・シグナリングはページと同じオリジンで提供される．
	// トンネル経由(https)なら wss，localhost 直接(http)なら ws で接続する．
	location := js.Global().Get("location")
	wsScheme := "ws"
	if location.Get("protocol").String() == "https:" {
		wsScheme = "wss"
	}
	host := location.Get("host").String()
	mmURL := url.URL{Scheme: wsScheme, Host: host, Path: "/matchmaking"}      // マッチメイキングサーバ用URL
	signalingURL := url.URL{Scheme: wsScheme, Host: host, Path: "/signaling"} // シグナリングサーバ用URL

	now := time.Now()           // タイムスタンプ取得
	userID, _ := shortHash(now) // ユーザID作成
	reqMsg, err := json.Marshal(mmReqMsg{
		UserID:    userID,
		CreatedAt: now,
	}) // リクエストメッセージ作成
	if err != nil {
		log.Fatal(err)
	}

	var resMsg mmResMsg        // レスポンスメッセージ
	var conn *ayame.Connection // go-ayameコネクション
	var inProgress atomic.Bool // マッチング〜接続処理の実行中フラグ(二重起動防止)

	js.Global().Set("NewVideoChat", js.FuncOf(func(_ js.Value, _ []js.Value) interface{} { // マッチング開始
		if !inProgress.CompareAndSwap(false, true) { // 実行中・接続済みなら何もしない
			logElem("[Sys]: Already started.\n")
			return js.Undefined()
		}
		go func() {
			connected := make(chan bool, 1)              // P2P接続完了チャンネル(ICE 再接続で複数回通知されても詰まらないようバッファ付き)
			dcReady := make(chan *webrtc.DataChannel, 1) // DataChannel 取得チャンネル(作成側・受信側のどちらか一方から 1 回)
			setDC := func(c *webrtc.DataChannel) {
				c.OnClose(handleError) // 切断時
				select {
				case dcReady <- c:
				default:
				}
			}
			// 失敗時は log.Fatal で WASM ごと終了させず，ログを出して再試行できるようにする
			fail := func(stage string, err error) {
				log.Printf("%s: %v", stage, err)
				logElem(fmt.Sprintf("[Sys]: %s failed. Please try again.\n", stage))
				inProgress.Store(false)
			}
			ws, _, err := websocket.Dial(context.Background(), mmURL.String(), nil) // マッチメイキングサーバ接続
			if err != nil {
				fail("Matchmaking", err)
				return
			}
			defer ws.Close(websocket.StatusNormalClosure, "close connection") // 関数を抜けたときセッションを閉じる

			if err := ws.Write(context.Background(), websocket.MessageText, reqMsg); err != nil { // テキスト形式でメッセージ送信
				fail("Matchmaking", err)
				return
			}
			logElem("[Sys]: Waiting match...\n") // ログ
			for {
				if err := wsjson.Read(context.Background(), ws, &resMsg); err != nil { // レスポンスメッセージ読み込み
					fail("Matchmaking", err)
					return
				}
				if resMsg.Type == "MATCH" { // 接続完了時
					break // ブレイク
				}
			}

			opts := ayame.DefaultOptions()                                                       // デフォルト設定(シグナリングサーバ)
			opts.Audio.Enabled = true                                                            // 音声有効
			opts.Audio.Direction = webrtc.RTPTransceiverDirectionSendrecv                        // 送受信有効
			opts.Video.Enabled = true                                                            // 映像有効
			opts.Video.Direction = webrtc.RTPTransceiverDirectionSendrecv                        // 送受信有効
			conn = ayame.NewConnection(signalingURL.String(), resMsg.RoomID, opts, false, false) // シグナリングサーバ接続(URL，ルームID，設定，デバッグ用(SDP 等をコンソールに出さない)，websocket自動切断有効可否)
			conn.OnOpen(func(metadata *interface{}) {                                            // Websocket接続時
				// 後から入室した側が DataChannel を作る(先に入室した側は OnDataChannel で受け取る)
				dc, err := conn.CreateDataChannel("video-chat", nil)
				if err != nil {
					return
				}
				setDC(dc)
			})

			conn.OnDataChannel(func(c *webrtc.DataChannel) { // DataChannel取得時
				setDC(c)
			})

			conn.OnConnect(func() { // P2Pコネクション接続時
				logElem("[Sys]: Matching! Start P2P video chat via DataChannel\n") // ログ
				conn.CloseWebSocketConnection()                                    // websocket接続閉じる
				select {
				case connected <- true: // P2Pコネクション接続完了通知
				default: // 通知済み(ICE 再接続など)
				}
			})

			if err := conn.Connect(); err != nil { // P2P通信接続開始
				fail("Signaling", err)
				return
			}
			// P2P 接続と DataChannel の両方がそろうのを待つ
			timeout := time.After(p2pConnectTimeout)
			var dc *webrtc.DataChannel
			for ok := false; !ok || dc == nil; {
				select {
				case <-connected: // P2P接続完了時
					ok = true
				case dc = <-dcReady: // DataChannel 取得時
				case <-timeout: // シグナリング失敗・NAT 越え失敗などで繋がらないとき
					conn.Disconnect()
					fail("P2P connection", fmt.Errorf("timeout after %s", p2pConnectTimeout))
					return
				}
			}
			raw, err := rawDataChannel(dc)
			if err != nil {
				conn.Disconnect()
				fail("DataChannel", err)
				return
			}
			// 映像・音声の送受信は JS 側で RTCDataChannel を直接使う．
			// Go(WASM) を経由すると 1 フレームごとに大きなデータのコピー・変換がメインスレッドで走り，
			// 特に WASM の最適化コンパイルが終わるまでの間に処理が追いつかず映像が固まるため．
			js.Global().Call("onP2PReady", raw) // JS側に接続完了を通知
		}()
		return js.Undefined() // 処理が終了しないよう未定義を返す
	}))
	select {} // 永続待機
}

func shortHash(now time.Time) (string, error) { // ユーザID作成
	h := sha256.New()                                        // SHA-256インスタンス
	if _, err := h.Write([]byte(now.String())); err != nil { // 時間をハッシュ化
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:7], nil // Sumで書き込み結果を抜き出して16進数文字列の先頭7文字取得
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

func handleError() { // エラー処理
	logElem("[Sys]: Maybe Any left, Please restart\n") // ログ
}

func getElementByID(id string) js.Value { // idに対応する要素取得
	return js.Global().Get("document").Call("getElementById", id) // 要素を返す
}
