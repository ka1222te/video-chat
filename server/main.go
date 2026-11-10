// Command server は Video Chat のローカルサーバ．
//
// 1 つのポートで以下を提供する (トンネルから見て単一オリジンになるようにする)．
//
//	/             public/ の静的ファイル (index.html, wasm_exec.js, video-chat.wasm)
//	/api/rooms    ルームの作成・確認・退出・削除 (JSON)
//	/room         入室と在室者一覧の通知 (WebSocket)
//	/signaling    参加者 2 人ずつの P2P 接続のシグナリング (WebSocket，Ayame 互換プロトコル)
//	/healthz      死活確認
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"
)

func main() {
	envPath := flag.String("env", ".env", ".env ファイルのパス")
	flag.Parse()

	cfg, err := LoadConfig(*envPath)
	if err != nil {
		log.Fatalf("設定の読み込みに失敗: %v", err)
	}
	if _, err := os.Stat(cfg.PublicDir + "/video-chat.wasm"); err != nil {
		log.Fatalf("%s/video-chat.wasm がありません．先に ./scripts/setup.sh でビルドしてください", cfg.PublicDir)
	}

	store := newRoomStore() // ルーム (ID + パスワード)．メモリ上のみで，再起動すると消える
	hub := newRoomHub(store)
	wsLimit := make(chan struct{}, maxWebSockets) // WebSocket 同時接続数の上限
	mux := http.NewServeMux()
	api := &roomAPI{store: store, hub: hub}
	mux.Handle("/api/rooms", api)
	mux.Handle("/api/rooms/", api) // verify / leave / delete
	mux.Handle("/room", limitConns(wsLimit, hub))
	mux.Handle("/signaling", limitConns(wsLimit, newSignaling(hub, cfg.iceServers())))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.Handle("/", noCache(http.FileServer(noDirListing{http.Dir(cfg.PublicDir)})))

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Println("停止します")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("Video Chat server listening on http://%s (public: %s)", cfg.ListenAddr, cfg.PublicDir)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// maxWebSockets は WebSocket の同時接続数の上限 (超えた分は 503)．
// 1 人あたり /room が 1 本と，接続を確立している間だけ相手ごとに /signaling が 1 本 (4 人なら最大 3 本)．
const maxWebSockets = 256

func limitConns(sem chan struct{}, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			h.ServeHTTP(w, r)
		default:
			http.Error(w, "too many connections", http.StatusServiceUnavailable)
		}
	})
}

// contentSecurityPolicy はページが読み込めるものを必要最小限にする．
//   - frame-ancestors 'none': 他サイトの iframe に埋め込ませない (カメラ許可のクリックジャッキング対策)
//   - img-src 'self' data:  : 相手から届いたフレームを外部 URL として読み込ませない
//   - connect-src 'self'    : WebSocket は同じオリジンのみ
//   - 'wasm-unsafe-eval'    : Go WASM の実行に必要
//   - 'unsafe-inline'       : index.html のインラインスクリプト・style 属性に必要
const contentSecurityPolicy = "default-src 'self'; " +
	"script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'; " +
	"style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; " +
	"connect-src 'self'; " +
	"media-src 'self' blob: mediastream:; " +
	"object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

func securityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hd := w.Header()
		hd.Set("Content-Security-Policy", contentSecurityPolicy)
		hd.Set("X-Content-Type-Options", "nosniff")
		hd.Set("X-Frame-Options", "DENY")
		hd.Set("Referrer-Policy", "no-referrer")
		hd.Set("Permissions-Policy", "camera=(self), microphone=(self), geolocation=(), payment=(), usb=()")
		h.ServeHTTP(w, r)
	})
}

// noDirListing はディレクトリ一覧を返さない (index.html があるディレクトリはそれを返す)．
type noDirListing struct{ fs http.FileSystem }

func (n noDirListing) Open(name string) (http.File, error) {
	f, err := n.fs.Open(name)
	if err != nil {
		return nil, err
	}
	if st, err := f.Stat(); err == nil && st.IsDir() {
		idx, err := n.fs.Open(path.Join(name, "index.html"))
		if err != nil {
			f.Close()
			return nil, os.ErrNotExist
		}
		idx.Close()
	}
	return f, nil
}

// noCache は再ビルドした video-chat.wasm などが確実に読み直されるようにする．
func noCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		h.ServeHTTP(w, r)
	})
}

// iceServers はシグナリングの accept メッセージでクライアントに渡す ICE サーバ一覧．
func (c *Config) iceServers() []iceServer {
	var servers []iceServer
	if len(c.STUNURLs) > 0 {
		servers = append(servers, iceServer{URLs: c.STUNURLs})
	}
	if len(c.TURNURLs) > 0 {
		servers = append(servers, iceServer{URLs: c.TURNURLs, Username: c.TURNUsername, Credential: c.TURNCredential})
	}
	return servers
}

// clientLabel はログ用の接続元表示 (gate-house 経由ならログインユーザのメールアドレス)．
func clientLabel(r *http.Request) string {
	if email := r.Header.Get("X-Auth-Email"); email != "" {
		return email
	}
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		return fwd
	}
	return r.RemoteAddr
}
