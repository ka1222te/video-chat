// このファイルは video-chat プロジェクトで追加したものです．

//go:build !(js && wasm)

package ayame

import "github.com/pion/webrtc/v3"

// setTransceiverDirections はブラウザ (js/wasm) 以外では何もしない．
func setTransceiverDirections(pc *webrtc.PeerConnection, opts *ConnectionOptions) {}
