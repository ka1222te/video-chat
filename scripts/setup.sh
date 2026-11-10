#!/usr/bin/env bash
# 初期セットアップ / 再ビルド (何度実行しても可)
#
#   ./scripts/setup.sh を直接実行すると:
#     - .env が無ければ .env.sample から作成 (既にあれば変更しない)
#     - .state/ と bin/ を作成
#     - 使用中の Go に対応する wasm_exec.js を public/ にコピー
#     - クライアント (public/video-chat.wasm) とサーバ (bin/video-chat-server) をビルド
#
#   start.sh / stop.sh からは source して共通の設定・関数だけを使う (ビルドはしない)。
set -euo pipefail

# ===== 共通設定 =====
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="$ROOT/.state"
BIN="$ROOT/bin/video-chat-server"
PID_FILE="$STATE/server.pid"
LOG_FILE="$STATE/server.log"
cd "$ROOT"

info() { printf '[+] %s\n' "$*"; }
warn() { printf '[!] %s\n' "$*" >&2; }
die()  { printf '[x] %s\n' "$*" >&2; exit 1; }

# .env の値を取得 (KEY='値' / KEY="値" / KEY=値 の形式に対応。無ければ既定値)
env_get() {
  local key="$1" def="${2:-}" val=""
  if [[ -f "$ROOT/.env" ]]; then
    val="$(sed -n -E "s/^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=[[:space:]]*['\"]?([^'\"#[:space:]]*).*/\2/p" "$ROOT/.env" | tail -n 1)"
  fi
  printf '%s' "${val:-$def}"
}

LISTEN_ADDR="$(env_get LISTEN_ADDR 127.0.0.1:8080)"
PORT="${LISTEN_ADDR##*:}"
HOST="${LISTEN_ADDR%:*}"
case "$HOST" in ""|0.0.0.0|"[::]") HOST=127.0.0.1 ;; esac

# go コマンドを探す (.env の GO → PATH → 標準的なインストール先)
find_go() {
  local c
  for c in "$(env_get GO)" "$(command -v go 2>/dev/null || true)" /usr/local/go/bin/go "$HOME/.local/go/bin/go" "$HOME/go/bin/go"; do
    [[ -n "$c" && -x "$c" ]] && { printf '%s' "$c"; return 0; }
  done
  return 1
}

# サーバが起動中なら PID を返す
running_pid() {
  local pid
  [[ -f "$PID_FILE" ]] || return 1
  pid="$(cat "$PID_FILE")"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && [[ "$(ps -p "$pid" -o comm= 2>/dev/null)" == video-chat-ser* ]] || return 1
  printf '%s' "$pid"
}

# /healthz が応答するか (curl 不要。bash の /dev/tcp を使う。接続失敗でスクリプトが終わらないようサブシェルで実行)
healthy() {
  (
    exec 3<>"/dev/tcp/$HOST/$PORT" || exit 1
    printf 'GET /healthz HTTP/1.0\r\nHost: %s:%s\r\n\r\n' "$HOST" "$PORT" >&3
    res="$(timeout 3 cat <&3 || true)"
    [[ "$res" == "HTTP/1."?" 200"* ]]
  ) 2>/dev/null
}

# ===== セットアップ本体 (直接実行したときのみ) =====
setup_main() {
  local go_bin
  go_bin="$(find_go)" || die "go が見つかりません。Go をインストールするか、.env に GO='/path/to/go' を設定してください"
  info "Go: $go_bin ($("$go_bin" version))"

  if [[ -f .env ]]; then
    info ".env: 既存のものを使用"
  else
    cp .env.sample .env
    chmod 600 .env
    info ".env を作成しました (必要に応じて編集してください)"
  fi

  mkdir -p bin "$STATE"
  chmod 700 "$STATE"

  cp "$("$go_bin" env GOROOT)/lib/wasm/wasm_exec.js" public/wasm_exec.js
  info "public/wasm_exec.js を更新"

  GOOS=js GOARCH=wasm "$go_bin" build -o public/video-chat.wasm ./client
  info "クライアントをビルド: public/video-chat.wasm"

  "$go_bin" build -o "$BIN" ./server
  info "サーバをビルド: bin/video-chat-server"

  echo
  if running_pid >/dev/null; then
    warn "サーバは起動中です。変更を反映するには ./scripts/stop.sh && ./scripts/start.sh を実行してください"
  else
    echo "次は ./scripts/start.sh で起動します"
  fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  setup_main "$@"
fi
