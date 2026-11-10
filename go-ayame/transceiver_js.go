// このファイルは video-chat プロジェクトで追加したものです．

//go:build js && wasm

package ayame

import "github.com/pion/webrtc/v3"

// setTransceiverDirections は受信した offer から作られた映像・音声のトランシーバの向きを
// Options に合わせる (既定では recvonly になり，受け手から送信できないため)．
func setTransceiverDirections(pc *webrtc.PeerConnection, opts *ConnectionOptions) {
	transceivers := pc.JSValue().Call("getTransceivers")
	for i := 0; i < transceivers.Length(); i++ {
		t := transceivers.Index(i)
		switch t.Get("receiver").Get("track").Get("kind").String() {
		case "audio":
			if opts.Audio.Enabled {
				t.Set("direction", opts.Audio.Direction.String())
			}
		case "video":
			if opts.Video.Enabled {
				t.Set("direction", opts.Video.Direction.String())
			}
		}
	}
}
