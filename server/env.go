package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Config はサーバの設定 (.env から読み込む)．
type Config struct {
	ListenAddr     string   // LISTEN_ADDR: 待受アドレス
	PublicDir      string   // PUBLIC_DIR: 配信する静的ファイルのディレクトリ
	STUNURLs       []string // STUN_URLS: クライアントに渡す STUN サーバ
	TURNURLs       []string // TURN_URLS: クライアントに渡す TURN サーバ (任意)
	TURNUsername   string   // TURN_USERNAME
	TURNCredential string   // TURN_CREDENTIAL
}

const (
	defaultListenAddr = "127.0.0.1:8080"
	defaultPublicDir  = "public"
	defaultSTUNURLs   = "stun:stun.l.google.com:19302"
)

// LoadConfig は path の .env を読み込む．ファイルが無ければ既定値で動作する．
// 同名の環境変数が設定されている場合はそちらを優先する．
func LoadConfig(path string) (*Config, error) {
	env := map[string]string{}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if env, err = parseEnv(data); err != nil {
			return nil, err
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	get := func(key, def string) string {
		if v, ok := os.LookupEnv(key); ok {
			return v
		}
		if v := env[key]; v != "" {
			return v
		}
		return def
	}
	return &Config{
		ListenAddr:     get("LISTEN_ADDR", defaultListenAddr),
		PublicDir:      get("PUBLIC_DIR", defaultPublicDir),
		STUNURLs:       splitList(get("STUN_URLS", defaultSTUNURLs)),
		TURNURLs:       splitList(get("TURN_URLS", "")),
		TURNUsername:   get("TURN_USERNAME", ""),
		TURNCredential: get("TURN_CREDENTIAL", ""),
	}, nil
}

// parseEnv は KEY=VALUE 形式の .env を解析する．
// 空行と # で始まる行は無視し，先頭の "export "，行末コメント，値を囲む引用符を取り除く．
func parseEnv(data []byte) (map[string]string, error) {
	env := make(map[string]string)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf(".env:%d: KEY=VALUE 形式ではありません: %q", lineNo, line)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) > 0 && (value[0] == '"' || value[0] == '\'') {
			if end := strings.IndexByte(value[1:], value[0]); end >= 0 {
				value = value[1 : end+1] // 引用符内はそのまま (# も値として扱う)
			}
		} else if i := strings.Index(value, " #"); i >= 0 {
			value = strings.TrimSpace(value[:i]) // 引用符なしの行末コメント
		}
		env[key] = value
	}
	return env, scanner.Err()
}

// splitList はカンマまたは空白区切りの文字列を分割する．
func splitList(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' })
}
