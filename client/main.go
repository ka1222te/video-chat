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

	"github.com/pion/webrtc/v3"
	"video-chat/go-ayame"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

const (
	msgTypeFrame = "frame" // フレームメッセージ(DataChannel用)
	msgTypeAudio = "audio" // 音声メッセージ(DataChannel用)

	p2pConnectTimeout = 60 * time.Second // マッチング後，P2P 接続の完了を待つ上限
	maxTextMessageLen = 500              // 相手から届く文字列メッセージをログに出す最大文字数
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

type mediaMessage struct { // メディアメッセージ(DataChannel用)
	Type      string `json:"type"`      // frame: dataURL 形式の画像，audio: base64 エンコードした PCM データ
	Data      string `json:"data"`      // データ
	Timestamp int64  `json:"timestamp"` // ミリ秒単位のタイムスタンプ(送信開始からの経過時間)
	Sequence  int64  `json:"sequence"`  // シーケンス番号(順序保証用)
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
	var dc *webrtc.DataChannel // Datachannel
	defer func() {             // 関数を抜けたとき，DataChannel解放
		if dc != nil {
			dc.Close()
		}
	}()

	var conn *ayame.Connection      // go-ayameコネクション
	connected := make(chan bool, 1) // P2P接続完了チャンネル(ICE 再接続で複数回通知されても詰まらないようバッファ付き)
	var inProgress atomic.Bool      // マッチング〜接続処理の実行中フラグ(二重起動防止)

	js.Global().Set("goSendLocalFrame", js.FuncOf(func(_ js.Value, args []js.Value) interface{} { // フレーム送信
		go func() {
			if len(args) < 3 { // dataURL，タイムスタンプ，シーケンス番号の情報がないとき
				return
			}
			if dc == nil { // DataChannelが未定義のとき
				return
			}
			dataURL := args[0].String() // データチャンネル
			timestamp := args[1].Int()  // タイムスタンプ
			sequence := args[2].Int()   // シーケンス番号
			msg := mediaMessage{
				Type:      msgTypeFrame,
				Data:      dataURL,
				Timestamp: int64(timestamp),
				Sequence:  int64(sequence),
			} // メディアメッセージ作成
			b, err := json.Marshal(msg) // 文字列に変換
			if err != nil {
				log.Printf("marshal frame error: %v", err)
				return
			}
			if err := dc.Send(b); err != nil { // データ送信
				log.Printf("send frame error: %v", err)
				handleError()
				return
			}
		}()
		return js.Undefined() // 処理が終了しないよう未定義を返す
	}))

	js.Global().Set("goSendAudioChunk", js.FuncOf(func(_ js.Value, args []js.Value) interface{} { // 音声チャンク送信
		go func() {
			if len(args) < 3 { // チャンクデータ．タイムスタンプ，シーケンス番号の情報がないとき
				return
			}
			if dc == nil { // DataChannelが未定義のとき
				return
			}
			base64Audio := args[0].String() // チャンクデータ
			timestamp := args[1].Int()      // タイムスタンプ
			sequence := args[2].Int()       // シーケンス番号
			msg := mediaMessage{
				Type:      msgTypeAudio,
				Data:      base64Audio,
				Timestamp: int64(timestamp),
				Sequence:  int64(sequence),
			} // メディアメッセージ作成
			b, err := json.Marshal(msg) // 文字列に変換
			if err != nil {
				log.Printf("marshal audio error: %v", err)
				return
			}
			if err := dc.Send(b); err != nil { // データ送信(SendTextではないため，受信側のmsg.IsStringはfalseになる)
				log.Printf("send audio error: %v", err)
				handleError()
				return
			}
		}()
		return js.Undefined() // 処理が終了しないよう未定義を返す
	}))

	js.Global().Set("NewVideoChat", js.FuncOf(func(_ js.Value, _ []js.Value) interface{} { // マッチング開始
		if !inProgress.CompareAndSwap(false, true) { // 実行中・接続済みなら何もしない
			logElem("[Sys]: Already started.\n")
			return js.Undefined()
		}
		go func() {
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
				log.Println("Open")                                           // デバッグ用
				var err error                                                 // エラー
				dc, err = conn.CreateDataChannel("video-chat", nil)           // DataChannel作成
				if err != nil && err != fmt.Errorf("client does not exist") { // 接続に問題があるとき
					log.Printf("CreateDataChannel error: %v", err)
					return
				}
				log.Printf("CreateDataChannel: label=%s", dc.Label()) // デバッグ用
				dc.OnMessage(onMessage())                             // メッセージ受信処理定義
			})

			conn.OnDataChannel(func(c *webrtc.DataChannel) { // DataChannel取得時
				log.Printf("OnDataChannel: label=%s", c.Label()) // デバッグ用
				if dc == nil {                                   // 未定義のとき
					dc = c // DataChannel更新
				}
				dc.OnMessage(onMessage()) // メッセージ受信処理定義
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
			select {
			case <-connected: // P2P接続完了時
				js.Global().Call("onP2PReady") // JS側に接続完了を通知
			case <-time.After(p2pConnectTimeout): // シグナリング失敗・NAT 越え失敗などで繋がらないとき
				conn.Disconnect()
				fail("P2P connection", fmt.Errorf("timeout after %s", p2pConnectTimeout))
			}
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

func onMessage() func(webrtc.DataChannelMessage) { // DataChannelのメッセージ受信処理
	return func(msg webrtc.DataChannelMessage) {
		if msg.IsString { // メッセージが文字列のとき
			text := []rune(string(msg.Data))
			if len(text) > maxTextMessageLen { // 相手から届く文字列は長さを制限してログに出す
				text = append(text[:maxTextMessageLen], '…')
			}
			logElem(fmt.Sprintf("[Any]: %s\n", string(text))) // ログ
			return
		}
		var m mediaMessage // 文字列でないときメディアメッセージとして扱う
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			log.Printf("unmarshal mediaMessage error: %v", err)
			return
		}
		switch m.Type { // メッセージタイプ確認
		case msgTypeFrame: // フレームのとき
			if !isAllowedFrameDataURL(m.Data) { // 画像の dataURL 以外(外部 URL など)は表示しない
				log.Printf("drop invalid frame from peer")
				return
			}
			js.Global().Call("renderRemoteFrame", m.Data, m.Timestamp, m.Sequence) // jsの関数でデータ，タイムスタンプ，シーケンス番号を渡す
		case msgTypeAudio: // 音声のとき
			js.Global().Call("playRemoteAudioChunk", m.Data, m.Timestamp, m.Sequence) // jsの関数でデータ，タイムスタンプ，シーケンス番号を渡す
		default: // それ以外のとき
			log.Printf("unknown media message type: %s", m.Type)
		}
	}
}

// isAllowedFrameDataURL は相手から届いたフレームが送信側の形式(WebP/JPEG の base64 dataURL)か確認する．
// 任意の URL を <img> に読み込ませると，相手が用意したサーバへのアクセス(IP アドレスの収集など)に使われるため．
func isAllowedFrameDataURL(s string) bool {
	return strings.HasPrefix(s, "data:image/webp;base64,") || strings.HasPrefix(s, "data:image/jpeg;base64,")
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
