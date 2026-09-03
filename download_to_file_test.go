package channel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 对齐 lark channel-sdk downloadResourceToFile：流式落盘、原子重命名、不整块占内存。
func TestDownloadFileToFile(t *testing.T) {
	media := strings.Repeat("dingtalk-media-bytes-", 500)
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			writeJSON(w, map[string]any{"accessToken": "tok-1", "expireIn": 7200})
		case "/v1.0/robot/messageFiles/download":
			writeJSON(w, map[string]any{"downloadUrl": srv.URL + "/media.bin"})
		case "/media.bin":
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte(media))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL, srv.URL)
	cfg.SSRFAllowlist = []string{"127.0.0.1"} // 测试回环地址须走白名单
	ch := New(cfg)

	dest := filepath.Join(t.TempDir(), "media.bin")
	n, err := ch.DownloadFileToFile(context.Background(), "dc-1", "m-1", "file", dest)
	if err != nil {
		t.Fatalf("DownloadFileToFile: %v", err)
	}
	if n != int64(len(media)) {
		t.Fatalf("bytes written = %d, want %d", n, len(media))
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(got) != media {
		t.Fatal("file content mismatch")
	}

	// DownloadFile 原有内存语义保持不变
	b, err := ch.DownloadFile(context.Background(), "dc-1", "m-1", "file")
	if err != nil || string(b) != media {
		t.Fatalf("DownloadFile broken after refactor: err=%v len=%d", err, len(b))
	}
}

func TestDownloadFileToFileErrors(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			writeJSON(w, map[string]any{"accessToken": "tok-1", "expireIn": 7200})
		case "/v1.0/robot/messageFiles/download":
			writeJSON(w, map[string]any{"downloadUrl": srv.URL + "/media.bin"})
		case "/media.bin":
			w.WriteHeader(http.StatusNotFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL, srv.URL)
	cfg.SSRFAllowlist = []string{"127.0.0.1"}
	ch := New(cfg)

	// 非 200 响应：错误信息应带可读状态码（回归：原实现 string(rune(404)) 会输出乱码）
	_, err := ch.DownloadFile(context.Background(), "dc-1", "m-1", "file")
	if err == nil || !strings.Contains(err.Error(), "http 404") {
		t.Fatalf("err = %v, want http 404 in message", err)
	}

	// 父目录不存在：报错且不创建目录
	missing := filepath.Join(t.TempDir(), "no-such-dir", "media.bin")
	if _, err := ch.DownloadFileToFile(context.Background(), "dc-1", "m-1", "file", missing); err == nil {
		t.Fatal("expected error for missing parent dir")
	}
	if _, statErr := os.Stat(filepath.Dir(missing)); !os.IsNotExist(statErr) {
		t.Fatal("parent dir must not be created")
	}

	// 空 destPath
	if _, err := ch.DownloadFileToFile(context.Background(), "dc-1", "m-1", "file", ""); err == nil {
		t.Fatal("expected error for empty destPath")
	}
}
