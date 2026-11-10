package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ルーム (ルームID + パスワード) の作成・確認・終了．
//
//	POST /api/rooms         {"password"}               -> 201 {"roomId", "password", "hostToken"}
//	POST /api/rooms/verify  {"roomId", "password"}     -> 200 / 403 / 404 / 409 (満室) / 429 (ロック中)
//	POST /api/rooms/leave   {"roomId", "hostToken"}    -> 200 (ホストの退出)
//	POST /api/rooms/delete  {"roomId", "hostToken"}    -> 200 (ホストによるルームの削除)
//
// パスワードを省略するとサーバが生成する．入室 (/room の join) でも同じ確認を行い，
// hostToken が正しければその参加者をホストとして扱う．
//
// ルームが削除されるのは次のとき．
//   - ホストが「ルームを削除して退出」したとき (delete) … 在室中の参加者も退室させる
//   - ホストが「退出」(leave) しており，在室人数が 0 になったとき
//   - (安全策) 在室人数 0 のまま roomIdleTTL が経ったとき … ホストが退出ボタンを押さずにタブを閉じた場合など

const (
	maxIDLen         = 64                                // クライアントが送る ID (ルームID・クライアントID) の最大長
	roomIDLen        = 10                                // 発行するルームIDの長さ
	minPasswordLen   = 4                                 // 利用者が指定するパスワードの最短文字数
	maxPasswordLen   = 64                                // パスワードの最長文字数
	genPasswordLen   = 12                                // 自動生成するパスワードの長さ
	roomIdleTTL      = 24 * time.Hour                    // 誰もいない状態でこの時間が経ったルームは削除する
	maxRooms         = 1000                              // 同時に存在できるルーム数の上限 (メモリ保護)
	maxFailures      = 10                                // この回数パスワードを間違えると
	failureWindow    = 10 * time.Minute                  // この期間ロックする (総当たり対策)
	passwordAlphabet = "23456789abcdefghjkmnpqrstuvwxyz" // 見間違えやすい 0/o/1/l/i を除く
	roomAPIBodyLimit = 4 << 10
	roomCapacity     = 4 // 1 ルームの最大人数 (全員が互いに P2P でつなぐため，人数が増えるほど各自の送信量が増える)
)

var (
	errRoomNotFound  = errors.New("room not found")
	errWrongPassword = errors.New("wrong password")
	errRoomLocked    = errors.New("too many failed attempts")
	errRoomFull      = errors.New("room is full")
	errNotHost       = errors.New("not the host")
	errTooManyRooms  = errors.New("too many rooms")
	errBadPassword   = errors.New("invalid password")
)

type room struct {
	salt       [16]byte
	hash       [32]byte    // sha256(salt || password)
	hostHash   [32]byte    // sha256(hostToken)
	occupants  int         // /room に接続中の人数 (通話中も接続を保つ)
	hostLeft   bool        // ホストが退出ボタンで退出した
	lastActive time.Time   // 最後に入室・退室があった時刻
	failures   []time.Time // 直近のパスワード誤り
}

// roomStore はルームをメモリ上に保持する (サーバを再起動すると消える)．
type roomStore struct {
	mu    sync.Mutex
	rooms map[string]*room
	now   func() time.Time // テスト用
}

func newRoomStore() *roomStore {
	return &roomStore{rooms: map[string]*room{}, now: time.Now}
}

// create はルームを作る．password が空ならサーバが生成する．hostToken はホストだけに渡す秘密の値．
func (s *roomStore) create(password string) (id, pw, hostToken string, err error) {
	if password == "" {
		password = randomPassword()
	} else if n := utf8.RuneCountInString(password); n < minPasswordLen || n > maxPasswordLen || !utf8.ValidString(password) {
		return "", "", "", errBadPassword
	}
	hostToken = rand.Text()
	r := &room{lastActive: s.now(), hostHash: sha256.Sum256([]byte(hostToken))}
	rand.Read(r.salt[:])
	r.hash = hashPassword(r.salt, password)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweep()
	if len(s.rooms) >= maxRooms {
		return "", "", "", errTooManyRooms
	}
	for {
		id = rand.Text()[:roomIDLen]
		if _, dup := s.rooms[id]; !dup {
			break
		}
	}
	s.rooms[id] = r
	return id, password, hostToken, nil
}

// checkPassword はパスワードを確認する (s.mu を持った状態で呼ぶ)．
func (s *roomStore) checkPassword(r *room, password string) error {
	now := s.now()
	recent := r.failures[:0]
	for _, t := range r.failures {
		if now.Sub(t) < failureWindow {
			recent = append(recent, t)
		}
	}
	r.failures = recent
	if len(r.failures) >= maxFailures {
		return errRoomLocked
	}
	if h := hashPassword(r.salt, password); subtle.ConstantTimeCompare(h[:], r.hash[:]) != 1 {
		r.failures = append(r.failures, now)
		return errWrongPassword
	}
	return nil
}

func (r *room) isHost(hostToken string) bool {
	h := sha256.Sum256([]byte(hostToken))
	return hostToken != "" && subtle.ConstantTimeCompare(h[:], r.hostHash[:]) == 1
}

// get はルームを取り出す (s.mu を持った状態で呼ぶ)．
func (s *roomStore) get(id string) (*room, error) {
	s.sweep()
	r, ok := s.rooms[id]
	if !ok {
		return nil, errRoomNotFound
	}
	return r, nil
}

// verify は参加前の確認 (ルームID・パスワード・空き)．
func (s *roomStore) verify(id, password string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.get(id)
	if err != nil {
		return err
	}
	if err := s.checkPassword(r, password); err != nil {
		return err
	}
	if r.occupants >= roomCapacity {
		return errRoomFull
	}
	return nil
}

// enter は入室する (/room の join 時)．退室時は必ず exit を呼ぶ．
func (s *roomStore) enter(id, password, hostToken string) (isHost bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.get(id)
	if err != nil {
		return false, err
	}
	if err := s.checkPassword(r, password); err != nil {
		return false, err
	}
	if r.occupants >= roomCapacity {
		return false, errRoomFull
	}
	r.occupants++
	r.lastActive = s.now()
	isHost = r.isHost(hostToken)
	if isHost {
		r.hostLeft = false // ホストが入り直した
	}
	return isHost, nil
}

// exit は退室する．ホストが退出済みで誰もいなくなったらルームを削除し，true を返す．
func (s *roomStore) exit(id string) (deleted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[id]
	if !ok { // 削除済み
		return false
	}
	r.occupants--
	r.lastActive = s.now()
	if r.hostLeft && r.occupants <= 0 {
		delete(s.rooms, id)
		return true
	}
	return false
}

// hostLeave はホストの退出を記録する．誰もいなければその場でルームを削除し，true を返す．
// (ホスト自身のシグナリング接続が切れたときの exit でも削除される)
func (s *roomStore) hostLeave(id, hostToken string) (deleted bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.get(id)
	if err != nil {
		return false, err
	}
	if !r.isHost(hostToken) {
		return false, errNotHost
	}
	r.hostLeft = true
	if r.occupants <= 0 {
		delete(s.rooms, id)
		return true, nil
	}
	return false, nil
}

// remove はホストの指示でルームを削除する．
func (s *roomStore) remove(id, hostToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.get(id)
	if err != nil {
		return err
	}
	if !r.isHost(hostToken) {
		return errNotHost
	}
	delete(s.rooms, id)
	return nil
}

// sweep は誰もいない状態で長く経ったルームを削除する (s.mu を持った状態で呼ぶ)．
func (s *roomStore) sweep() {
	now := s.now()
	for id, r := range s.rooms {
		if r.occupants <= 0 && now.Sub(r.lastActive) > roomIdleTTL {
			delete(s.rooms, id)
		}
	}
}

func hashPassword(salt [16]byte, password string) [32]byte {
	return sha256.Sum256(append(salt[:], password...))
}

func randomPassword() string {
	b := make([]byte, genPasswordLen)
	for i := range b {
		b[i] = passwordAlphabet[randIntn(len(passwordAlphabet))]
	}
	return string(b)
}

// randIntn は [0, n) の一様乱数 (crypto/rand)．
func randIntn(n int) int {
	var b [1]byte
	limit := 256 - 256%n // 偏りが出ないよう端数は捨てる
	for {
		rand.Read(b[:])
		if int(b[0]) < limit {
			return int(b[0]) % n
		}
	}
}

// normalizeRoomID は入力されたルームIDを比較用の形にする (大文字化・空白とハイフンの除去)．
func normalizeRoomID(id string) string {
	return strings.Map(func(r rune) rune {
		if r == ' ' || r == '-' {
			return -1
		}
		return r
	}, strings.ToUpper(strings.TrimSpace(id)))
}

// roomAPI は /api/rooms 以下を処理する．
type roomAPI struct {
	store *roomStore
	hub   *roomHub // ルーム削除時に在室者を退室させる
}

func (a *roomAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	// 他サイトからのリクエストを受け付けない (JSON 必須にしてフォーム送信も防ぐ)
	if !sameOrigin(r) || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		log.Printf("rooms: forbidden %s origin=%q host=%q forwarded-host=%q content-type=%q (%q)", r.URL.Path,
			r.Header.Get("Origin"), r.Host, r.Header.Get("X-Forwarded-Host"), r.Header.Get("Content-Type"), clientLabel(r))
		jsonError(w, http.StatusForbidden, "forbidden")
		return
	}
	var req struct {
		RoomID    string `json:"roomId"`
		Password  string `json:"password"`
		HostToken string `json:"hostToken"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, roomAPIBodyLimit)).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid request")
		return
	}
	who := clientLabel(r)
	id := normalizeRoomID(req.RoomID)

	switch r.URL.Path {
	case "/api/rooms":
		id, pw, token, err := a.store.create(req.Password)
		switch {
		case errors.Is(err, errBadPassword):
			jsonError(w, http.StatusBadRequest, "INVALID-PASSWORD-FORMAT")
			return
		case err != nil:
			log.Printf("rooms: create failed (%q): %v", who, err)
			jsonError(w, http.StatusServiceUnavailable, "TOO-MANY-ROOMS")
			return
		}
		log.Printf("rooms: created room=%s (%q)", id, who)
		writeJSON(w, http.StatusCreated, map[string]string{"roomId": id, "password": pw, "hostToken": token})
	case "/api/rooms/verify":
		if err := a.store.verify(id, req.Password); err != nil {
			log.Printf("rooms: verify failed room=%q (%q): %v", id, who, err)
			status, reason := rejectStatus(err)
			jsonError(w, status, reason)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "roomId": id})
	case "/api/rooms/leave":
		deleted, err := a.store.hostLeave(id, req.HostToken)
		if err != nil {
			status, reason := rejectStatus(err)
			jsonError(w, status, reason)
			return
		}
		log.Printf("rooms: host left room=%s deleted=%v (%q)", id, deleted, who)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": deleted})
	case "/api/rooms/delete":
		if err := a.store.remove(id, req.HostToken); err != nil {
			status, reason := rejectStatus(err)
			jsonError(w, status, reason)
			return
		}
		a.hub.closeRoom(id, "ROOM-DELETED")
		log.Printf("rooms: deleted room=%s by host (%q)", id, who)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": true})
	default:
		jsonError(w, http.StatusNotFound, "not found")
	}
}

// rejectStatus はルーム操作のエラーを HTTP ステータスとシグナリングの reject 理由に変換する．
func rejectStatus(err error) (int, string) {
	switch {
	case errors.Is(err, errRoomNotFound):
		return http.StatusNotFound, "INVALID-ROOM-ID"
	case errors.Is(err, errRoomLocked):
		return http.StatusTooManyRequests, "LOCKED"
	case errors.Is(err, errRoomFull):
		return http.StatusConflict, "ROOM-FULL"
	case errors.Is(err, errNotHost):
		return http.StatusForbidden, "NOT-HOST"
	default:
		return http.StatusForbidden, "INVALID-PASSWORD"
	}
}

// sameOrigin は Origin ヘッダがあれば，このサーバのページからのリクエストか確認する．
// トンネル経由では Host が中継先 (127.0.0.1:8080) に書き換えられ，Origin は公開 URL のまま届く．
// gate-house は公開 URL のホストを X-Forwarded-Host に設定する (利用者が送った値は捨てる) ため，そちらとも比較する．
// LISTEN_ADDR を 127.0.0.1 にしておけば，トンネルを通らずに X-Forwarded-Host を偽装されることはない．
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	if strings.EqualFold(u.Host, r.Host) {
		return true
	}
	fwd := r.Header.Get("X-Forwarded-Host")
	return fwd != "" && strings.EqualFold(u.Host, strings.TrimSpace(strings.Split(fwd, ",")[0]))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	w.Write(marshal(v))
}

func jsonError(w http.ResponseWriter, status int, reason string) {
	writeJSON(w, status, map[string]string{"error": reason})
}

// marshal は送信用の JSON を作る (固定の構造体・map のみ渡すためエラーは起きない)．
func marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
