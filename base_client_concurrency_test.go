package polymarketrealtime

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestBaseClientPingDoesNotBlockWhenWriteChanFull(t *testing.T) {
	client := newBaseClient(
		NewRealtimeProtocol(),
		WithAutoReconnect(false),
		WithLogger(NewSilentLogger()),
	)

	for i := 0; i < cap(client.writeChan); i++ {
		client.writeChan <- writeRequest{messageType: websocket.TextMessage, data: []byte("queued")}
	}

	done := make(chan struct{})
	go func() {
		client.ping()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ping blocked while the write channel was full")
	}
}

func TestBaseClientDisconnectDoesNotBlockWhenWriteChanFull(t *testing.T) {
	wsURL, cleanup, _, _ := newTestWebSocketServer(t)
	defer cleanup()

	client := newBaseClient(
		NewRealtimeProtocol(),
		WithHost(wsURL),
		WithAutoReconnect(false),
		WithWriteTimeout(100*time.Millisecond),
		WithLogger(NewSilentLogger()),
	)

	for i := 0; i < cap(client.writeChan); i++ {
		client.writeChan <- writeRequest{messageType: websocket.TextMessage, data: []byte("queued")}
	}

	if err := client.connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- client.disconnect()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("disconnect failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("disconnect blocked while the write channel was full")
	}
}

func TestBaseClientWriteLoopSurvivesRecoverableWriteError(t *testing.T) {
	firstURL, cleanupFirst, firstConnCh, _ := newTestWebSocketServer(t)
	defer cleanupFirst()

	client := newBaseClient(
		NewRealtimeProtocol(),
		WithHost(firstURL),
		WithAutoReconnect(false),
		WithWriteTimeout(100*time.Millisecond),
		WithReadTimeout(100*time.Millisecond),
		WithLogger(NewSilentLogger()),
	)

	if err := client.connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	var firstServerConn *websocket.Conn
	select {
	case firstServerConn = <-firstConnCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first server connection")
	}

	if err := firstServerConn.UnderlyingConn().Close(); err != nil {
		t.Fatalf("failed to force-close first server connection: %v", err)
	}

	if err := client.enqueueWrite(
		writeRequest{messageType: websocket.TextMessage, data: []byte("first")},
		time.Second,
		"sending first test message",
	); err != nil {
		t.Fatalf("failed to enqueue first write: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	secondURL, cleanupSecond, _, secondMsgCh := newTestWebSocketServer(t)
	defer cleanupSecond()

	secondConn, _, err := websocket.DefaultDialer.Dial(secondURL, nil)
	if err != nil {
		t.Fatalf("failed to connect second websocket: %v", err)
	}
	defer secondConn.Close()

	client.connMu.Lock()
	client.conn = secondConn
	client.connMu.Unlock()

	client.internal.mu.Lock()
	client.internal.connClosed = false
	client.internal.mu.Unlock()

	if err := client.enqueueWrite(
		writeRequest{messageType: websocket.TextMessage, data: []byte("second")},
		time.Second,
		"sending second test message",
	); err != nil {
		t.Fatalf("failed to enqueue second write: %v", err)
	}

	select {
	case msg := <-secondMsgCh:
		if string(msg) != "second" {
			t.Fatalf("unexpected second message payload: %q", string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the write loop to handle the replacement connection")
	}

	client.closeOnce.Do(func() {
		close(client.closeChan)
	})
}

func newTestWebSocketServer(t *testing.T) (string, func(), <-chan *websocket.Conn, <-chan []byte) {
	t.Helper()

	connCh := make(chan *websocket.Conn, 1)
	msgCh := make(chan []byte, 1)
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}

		connCh <- conn

		go func() {
			defer conn.Close()
			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					return
				}

				select {
				case msgCh <- message:
				default:
				}
			}
		}()
	}))

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	return wsURL, server.Close, connCh, msgCh
}
