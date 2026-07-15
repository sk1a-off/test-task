package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sync"
	"testing"
	"time"
)

type getResult struct {
	message string
	ok      bool
}

func request(h http.Handler, method, target string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func requireResponse(t *testing.T, w *httptest.ResponseRecorder, code int, body string) {
	t.Helper()
	if w.Code != code || w.Body.String() != body {
		t.Fatalf("response = (%d, %q), want (%d, %q)", w.Code, w.Body.String(), code, body)
	}
}

func asyncGet(ctx context.Context, b *broker, name string, timeout time.Duration) <-chan getResult {
	result := make(chan getResult, 1)
	go func() {
		message, ok := b.get(ctx, name, timeout)
		result <- getResult{message: message, ok: ok}
	}()
	return result
}

func receiveResult(t *testing.T, result <-chan getResult) getResult {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case value := <-result:
		return value
	case <-timer.C:
		t.Fatal("get did not finish")
		return getResult{}
	}
}

func waiterCount(b *broker, name string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queues[name]
	if q == nil {
		return 0
	}
	return q.waiters.Len()
}

func waitForWaiters(t *testing.T, b *broker, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for waiterCount(b, name) != want {
		if time.Now().After(deadline) {
			t.Fatalf("waiters for %q = %d, want %d", name, waiterCount(b, name), want)
		}
		runtime.Gosched()
	}
}

func requireQueueAbsent(t *testing.T, b *broker, name string) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.queues[name]; ok {
		t.Fatalf("queue %q was not removed", name)
	}
}

func waitGroup(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Fatal("concurrent operations did not finish")
	}
}

func expectedMessages(total int) map[string]struct{} {
	expected := make(map[string]struct{}, total)
	for i := range total {
		expected[fmt.Sprintf("message-%d", i)] = struct{}{}
	}
	return expected
}

func requireExpectedMessage(t *testing.T, expected map[string]struct{}, message string) {
	t.Helper()
	if _, ok := expected[message]; !ok {
		t.Fatalf("unexpected or duplicate message %q", message)
	}
	delete(expected, message)
}

func TestHandlerPutGetFIFO(t *testing.T) {
	h := handler{broker: newBroker()}
	requireResponse(t, request(h, http.MethodPut, "/pet?v=cat"), http.StatusOK, "")
	requireResponse(t, request(h, http.MethodPut, "/pet?v=dog"), http.StatusOK, "")
	w := request(h, http.MethodGet, "/pet")
	requireResponse(t, w, http.StatusOK, "cat")
	if contentType := w.Header().Get("Content-Type"); contentType != "text/plain; charset=utf-8" {
		t.Fatalf("Content-Type = %q", contentType)
	}
	if cacheControl := w.Header().Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("Cache-Control = %q", cacheControl)
	}
	requireResponse(t, request(h, http.MethodGet, "/pet"), http.StatusOK, "dog")
	requireResponse(t, request(h, http.MethodGet, "/pet"), http.StatusNotFound, "")
}

func TestHandlerMissingAndEmptyValue(t *testing.T) {
	h := handler{broker: newBroker()}
	requireResponse(t, request(h, http.MethodPut, "/empty"), http.StatusBadRequest, "")

	for _, target := range []string{"/empty?v=", "/empty?v"} {
		requireResponse(t, request(h, http.MethodPut, target), http.StatusOK, "")
		w := request(h, http.MethodGet, "/empty")
		requireResponse(t, w, http.StatusOK, "")
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("empty message response is cacheable")
		}
	}
	requireResponse(t, request(h, http.MethodPut, "/value?v=first&v=second"), http.StatusOK, "")
	requireResponse(t, request(h, http.MethodGet, "/value"), http.StatusOK, "first")
}

func TestHandlerQueueIsolationEncodedAndRepeatedMessages(t *testing.T) {
	h := handler{broker: newBroker()}
	queuePath := "/" + url.PathEscape("домашние животные")
	messages := []string{"hello world", "кот+dog & 100% = yes / ok", "same", "same"}
	for _, message := range messages {
		target := queuePath + "?v=" + url.QueryEscape(message)
		requireResponse(t, request(h, http.MethodPut, target), http.StatusOK, "")
	}
	requireResponse(t, request(h, http.MethodPut, "/role?v=manager"), http.StatusOK, "")

	for _, message := range messages {
		requireResponse(t, request(h, http.MethodGet, queuePath), http.StatusOK, message)
	}
	requireResponse(t, request(h, http.MethodGet, "/role"), http.StatusOK, "manager")
	requireResponse(t, request(h, http.MethodGet, queuePath), http.StatusNotFound, "")
}

func TestHandlerPathAndMethodPolicy(t *testing.T) {
	h := handler{broker: newBroker()}
	requireResponse(t, request(h, http.MethodGet, "/"), http.StatusNotFound, "")

	w := request(h, http.MethodPost, "/pet")
	requireResponse(t, w, http.StatusMethodNotAllowed, "")
	if allow := w.Header().Get("Allow"); allow != "GET, PUT" {
		t.Fatalf("Allow = %q, want %q", allow, "GET, PUT")
	}

	requireResponse(t, request(h, http.MethodPut, "/team/backend?v=go"), http.StatusOK, "")
	requireResponse(t, request(h, http.MethodGet, "/team/backend"), http.StatusOK, "go")
	requireResponse(t, request(h, http.MethodPut, "/encoded%2Fslash?v=value"), http.StatusOK, "")
	requireResponse(t, request(h, http.MethodGet, "/encoded/slash"), http.StatusOK, "value")
}

func TestHandlerErrorBodiesAreEmpty(t *testing.T) {
	h := handler{broker: newBroker()}
	tests := []struct {
		method string
		target string
		code   int
	}{
		{http.MethodPut, "/q", http.StatusBadRequest},
		{http.MethodGet, "/q", http.StatusNotFound},
		{http.MethodGet, "/q?timeout=0", http.StatusNotFound},
		{http.MethodGet, "/q?timeout=bad", http.StatusBadRequest},
		{http.MethodGet, "/q?timeout=1&timeout=2", http.StatusBadRequest},
	}
	for _, test := range tests {
		w := request(h, test.method, test.target)
		requireResponse(t, w, test.code, "")
		if test.method == http.MethodGet && w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("GET %s is cacheable", test.target)
		}
	}
}

func TestHandlerRejectsMalformedQueryWithoutChangingQueue(t *testing.T) {
	h := handler{broker: newBroker()}
	requireResponse(t, request(h, http.MethodPut, "/q?v=kept"), http.StatusOK, "")
	requireResponse(t, request(h, http.MethodGet, "/q?timeout=1;bad=2"), http.StatusBadRequest, "")
	requireResponse(t, request(h, http.MethodGet, "/q"), http.StatusOK, "kept")

	requireResponse(t, request(h, http.MethodPut, "/other?v=bad&x=a;b"), http.StatusBadRequest, "")
	requireResponse(t, request(h, http.MethodGet, "/other"), http.StatusNotFound, "")

	r := httptest.NewRequest(http.MethodGet, "/q", nil)
	r.URL.RawQuery = "timeout=%zz"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	requireResponse(t, w, http.StatusBadRequest, "")
}

func TestRequestTimeout(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		want    time.Duration
		wantErr bool
	}{
		{"absent", "/q", 0, false},
		{"zero", "/q?timeout=0", 0, false},
		{"positive", "/q?timeout=7", 7 * time.Second, false},
		{"encoded plus", "/q?timeout=%2B1", 0, true},
		{"empty", "/q?timeout=", 0, true},
		{"negative", "/q?timeout=-1", 0, true},
		{"fraction", "/q?timeout=1.5", 0, true},
		{"text", "/q?timeout=soon", 0, true},
		{"malformed separator", "/q?timeout=1;bad=2", 0, true},
		{"duration maximum seconds", "/q?timeout=9223372036", 9223372036 * time.Second, false},
		{"duration overflow", "/q?timeout=9223372037", 0, true},
		{"uint64 overflow", "/q?timeout=18446744073709551616", 0, true},
		{"duplicate", "/q?timeout=1&timeout=2", 0, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, test.target, nil)
			got, err := requestTimeout(r)
			if (err != nil) != test.wantErr {
				t.Fatalf("requestTimeout error = %v, wantErr %v", err, test.wantErr)
			}
			if !test.wantErr && got != test.want {
				t.Fatalf("requestTimeout = %v, want %v", got, test.want)
			}
		})
	}
}

func TestHandlerExistingMessageWithTimeout(t *testing.T) {
	h := handler{broker: newBroker()}
	requireResponse(t, request(h, http.MethodPut, "/q?v=ready"), http.StatusOK, "")
	requireResponse(t, request(h, http.MethodGet, "/q?timeout=10"), http.StatusOK, "ready")
}

func TestHandlerDelayedDelivery(t *testing.T) {
	b := newBroker()
	h := handler{broker: b}
	r := httptest.NewRequest(http.MethodGet, "/q?timeout=2", nil)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, r)
		close(done)
	}()

	waitForWaiters(t, b, "q", 1)
	requireResponse(t, request(h, http.MethodPut, "/q?v=later"), http.StatusOK, "")
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("long-polling handler did not finish")
	}
	requireResponse(t, w, http.StatusOK, "later")
}

func TestHandlerWaiterFIFO(t *testing.T) {
	b := newBroker()
	h := handler{broker: b}
	startGet := func() (*httptest.ResponseRecorder, <-chan struct{}) {
		r := httptest.NewRequest(http.MethodGet, "/q?timeout=2", nil)
		w := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			h.ServeHTTP(w, r)
			close(done)
		}()
		return w, done
	}

	first, firstDone := startGet()
	waitForWaiters(t, b, "q", 1)
	second, secondDone := startGet()
	waitForWaiters(t, b, "q", 2)
	b.put("q", "cat")
	b.put("q", "dog")
	for _, done := range []<-chan struct{}{firstDone, secondDone} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("waiting handler did not finish")
		}
	}
	requireResponse(t, first, http.StatusOK, "cat")
	requireResponse(t, second, http.StatusOK, "dog")
}

func TestHandlerTimeoutExpires(t *testing.T) {
	b := newBroker()
	h := handler{broker: b}
	r := httptest.NewRequest(http.MethodGet, "/q?timeout=1", nil)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, r)
		close(done)
	}()
	waitForWaiters(t, b, "q", 1)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout handler did not finish")
	}
	requireResponse(t, w, http.StatusNotFound, "")
	requireQueueAbsent(t, b, "q")
}

func TestHandlerCancellation(t *testing.T) {
	b := newBroker()
	h := handler{broker: b}
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "/q?timeout=10", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, r)
		close(done)
	}()

	waitForWaiters(t, b, "q", 1)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled handler did not finish")
	}
	requireResponse(t, w, http.StatusNotFound, "")
	requireQueueAbsent(t, b, "q")
}

func TestBrokerWaiterFIFO(t *testing.T) {
	b := newBroker()
	first := asyncGet(context.Background(), b, "q", 5*time.Second)
	waitForWaiters(t, b, "q", 1)
	second := asyncGet(context.Background(), b, "q", 5*time.Second)
	waitForWaiters(t, b, "q", 2)

	b.put("q", "cat")
	b.put("q", "dog")
	if got := receiveResult(t, first); got != (getResult{"cat", true}) {
		t.Fatalf("first waiter = %#v, want cat", got)
	}
	if got := receiveResult(t, second); got != (getResult{"dog", true}) {
		t.Fatalf("second waiter = %#v, want dog", got)
	}
}

func TestBrokerFirstWaiterTimesOut(t *testing.T) {
	b := newBroker()
	first := asyncGet(context.Background(), b, "q", 100*time.Millisecond)
	waitForWaiters(t, b, "q", 1)
	second := asyncGet(context.Background(), b, "q", 5*time.Second)
	waitForWaiters(t, b, "q", 2)

	if got := receiveResult(t, first); got.ok {
		t.Fatalf("timed-out waiter received %#v", got)
	}
	waitForWaiters(t, b, "q", 1)
	b.put("q", "next")
	if got := receiveResult(t, second); got != (getResult{"next", true}) {
		t.Fatalf("second waiter = %#v, want next", got)
	}
}

func TestBrokerCanceledWaitersDoNotBlock(t *testing.T) {
	b := newBroker()
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first := asyncGet(firstCtx, b, "q", time.Hour)
	waitForWaiters(t, b, "q", 1)
	second := asyncGet(context.Background(), b, "q", 5*time.Second)
	waitForWaiters(t, b, "q", 2)

	cancelFirst()
	if got := receiveResult(t, first); got.ok {
		t.Fatalf("canceled waiter received %#v", got)
	}
	waitForWaiters(t, b, "q", 1)
	b.put("q", "next")
	if got := receiveResult(t, second); got != (getResult{"next", true}) {
		t.Fatalf("active waiter = %#v, want next", got)
	}
}

func TestBrokerCanceledMiddleWaiter(t *testing.T) {
	b := newBroker()
	first := asyncGet(context.Background(), b, "q", 5*time.Second)
	waitForWaiters(t, b, "q", 1)
	middleCtx, cancelMiddle := context.WithCancel(context.Background())
	middle := asyncGet(middleCtx, b, "q", 5*time.Second)
	waitForWaiters(t, b, "q", 2)
	last := asyncGet(context.Background(), b, "q", 5*time.Second)
	waitForWaiters(t, b, "q", 3)

	cancelMiddle()
	if got := receiveResult(t, middle); got.ok {
		t.Fatalf("canceled middle waiter received %#v", got)
	}
	waitForWaiters(t, b, "q", 2)
	b.put("q", "one")
	b.put("q", "two")
	if got := receiveResult(t, first); got != (getResult{"one", true}) {
		t.Fatalf("first waiter = %#v", got)
	}
	if got := receiveResult(t, last); got != (getResult{"two", true}) {
		t.Fatalf("last waiter = %#v", got)
	}
}

func TestBrokerManyCanceledWaitersAreCleaned(t *testing.T) {
	const total = 200
	b := newBroker()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	results := make([]<-chan getResult, 0, total)
	for range total {
		results = append(results, asyncGet(ctx, b, "q", 5*time.Second))
	}
	waitForWaiters(t, b, "q", total)

	cancel()
	for _, result := range results {
		if got := receiveResult(t, result); got.ok {
			t.Fatalf("canceled waiter received %#v", got)
		}
	}
	waitForWaiters(t, b, "q", 0)
	requireQueueAbsent(t, b, "q")
}

func TestBrokerPutSkipsAlreadyDoneWaiter(t *testing.T) {
	b := newBroker()
	q := &queue{}
	b.queues["q"] = q
	deadDone := make(chan struct{})
	close(deadDone)
	dead := &waiter{ch: make(chan string, 1), done: deadDone, state: waiterWaiting}
	dead.element = q.waiters.PushBack(dead)
	live := &waiter{ch: make(chan string, 1), done: make(chan struct{}), state: waiterWaiting}
	live.element = q.waiters.PushBack(live)

	b.put("q", "kept")
	if dead.state != waiterCanceled {
		t.Fatalf("dead waiter state = %v, want canceled", dead.state)
	}
	select {
	case message := <-dead.ch:
		t.Fatalf("dead waiter received %q", message)
	default:
	}
	select {
	case message := <-live.ch:
		if message != "kept" {
			t.Fatalf("live waiter received %q", message)
		}
	default:
		t.Fatal("live waiter did not receive message")
	}
	requireQueueAbsent(t, b, "q")
}

func TestDeleteIfEmptyDoesNotDeleteReplacement(t *testing.T) {
	b := newBroker()
	oldQueue := &queue{}
	newQueue := &queue{messages: []string{"kept"}}
	b.queues["q"] = newQueue
	b.mu.Lock()
	b.deleteIfEmpty("q", oldQueue)
	b.mu.Unlock()
	message, ok := b.get(context.Background(), "q", 0)
	if !ok || message != "kept" {
		t.Fatalf("replacement queue message = (%q, %v)", message, ok)
	}
}

func TestCanceledContextDoesNotConsumeReadyMessage(t *testing.T) {
	b := newBroker()
	b.put("q", "kept")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if message, ok := b.get(ctx, "q", time.Second); ok {
		t.Fatalf("canceled get consumed %q", message)
	}
	if message, ok := b.get(context.Background(), "q", 0); !ok || message != "kept" {
		t.Fatalf("next get = (%q, %v), want kept", message, ok)
	}
}

func TestConcurrentPutNoLoss(t *testing.T) {
	const total = 1000
	const producers = 20
	b := newBroker()
	var wg sync.WaitGroup
	for producer := 0; producer < producers; producer++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			for i := producer; i < total; i += producers {
				b.put("q", fmt.Sprintf("message-%d", i))
			}
		}(producer)
	}
	waitGroup(t, &wg)

	expected := expectedMessages(total)
	for range total {
		message, ok := b.get(context.Background(), "q", 0)
		if !ok {
			t.Fatal("message was lost")
		}
		requireExpectedMessage(t, expected, message)
	}
	if _, ok := b.get(context.Background(), "q", 0); ok || len(expected) != 0 {
		t.Fatalf("%d expected messages remain", len(expected))
	}
}

func TestConcurrentGetNoDuplicates(t *testing.T) {
	const total = 1000
	const extraConsumers = 50
	b := newBroker()
	for i := range total {
		b.put("q", fmt.Sprintf("message-%d", i))
	}

	results := make(chan getResult, total+extraConsumers)
	var wg sync.WaitGroup
	for range total + extraConsumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			message, ok := b.get(context.Background(), "q", 0)
			results <- getResult{message, ok}
		}()
	}
	waitGroup(t, &wg)
	close(results)

	expected := expectedMessages(total)
	misses := 0
	for result := range results {
		if !result.ok {
			misses++
			continue
		}
		requireExpectedMessage(t, expected, result.message)
	}
	if len(expected) != 0 || misses != extraConsumers {
		t.Fatalf("remaining messages = %d, misses = %d", len(expected), misses)
	}
}

func TestConcurrentProducersAndConsumers(t *testing.T) {
	const total = 1000
	const workers = 20
	b := newBroker()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan getResult, total)

	var consumers sync.WaitGroup
	for range workers {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for range total / workers {
				message, ok := b.get(ctx, "q", 5*time.Second)
				results <- getResult{message, ok}
			}
		}()
	}

	var producers sync.WaitGroup
	for producer := range workers {
		producers.Add(1)
		go func(producer int) {
			defer producers.Done()
			for i := producer; i < total; i += workers {
				b.put("q", fmt.Sprintf("message-%d", i))
			}
		}(producer)
	}
	waitGroup(t, &producers)
	waitGroup(t, &consumers)
	close(results)

	expected := expectedMessages(total)
	for result := range results {
		if !result.ok {
			t.Fatalf("message was lost: %#v", result)
		}
		requireExpectedMessage(t, expected, result.message)
	}
	if len(expected) != 0 {
		t.Fatalf("%d expected messages remain", len(expected))
	}
	requireQueueAbsent(t, b, "q")
}

func TestConcurrentIndependentQueues(t *testing.T) {
	const queueCount = 4
	const perQueue = 200
	b := newBroker()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type namedResult struct {
		queue   int
		message string
		ok      bool
	}
	results := make(chan namedResult, queueCount*perQueue)
	var consumers sync.WaitGroup
	for queueNumber := range queueCount {
		consumers.Add(1)
		go func(queueNumber int) {
			defer consumers.Done()
			name := fmt.Sprintf("queue-%d", queueNumber)
			for range perQueue {
				message, ok := b.get(ctx, name, 5*time.Second)
				results <- namedResult{queueNumber, message, ok}
			}
		}(queueNumber)
	}

	var producers sync.WaitGroup
	for queueNumber := range queueCount {
		producers.Add(1)
		go func(queueNumber int) {
			defer producers.Done()
			name := fmt.Sprintf("queue-%d", queueNumber)
			for i := range perQueue {
				b.put(name, fmt.Sprintf("%d-%d", queueNumber, i))
			}
		}(queueNumber)
	}
	waitGroup(t, &producers)
	waitGroup(t, &consumers)
	close(results)

	expected := make(map[string]struct{}, queueCount*perQueue)
	for queueNumber := range queueCount {
		for i := range perQueue {
			key := fmt.Sprintf("%d/%d-%d", queueNumber, queueNumber, i)
			expected[key] = struct{}{}
		}
	}
	for result := range results {
		key := fmt.Sprintf("%d/%s", result.queue, result.message)
		if !result.ok {
			t.Fatalf("invalid cross-queue result %#v", result)
		}
		requireExpectedMessage(t, expected, key)
	}
	if len(expected) != 0 {
		t.Fatalf("%d cross-queue messages remain", len(expected))
	}
	for queueNumber := range queueCount {
		requireQueueAbsent(t, b, fmt.Sprintf("queue-%d", queueNumber))
	}
}

func TestCancelDeliveryRaceConservesMessage(t *testing.T) {
	const iterations = 100
	for i := range iterations {
		b := newBroker()
		ctx, cancel := context.WithCancel(context.Background())
		result := asyncGet(ctx, b, "q", time.Second)
		waitForWaiters(t, b, "q", 1)
		message := fmt.Sprintf("message-%d", i)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			cancel()
		}()
		go func() {
			defer wg.Done()
			<-start
			b.put("q", message)
		}()
		close(start)
		waitGroup(t, &wg)

		got := receiveResult(t, result)
		left, leftOK := b.get(context.Background(), "q", 0)
		if got.ok {
			if got.message != message || leftOK {
				t.Fatalf("iteration %d: delivered=%#v, queued=(%q,%v)", i, got, left, leftOK)
			}
		} else if !leftOK || left != message {
			t.Fatalf("iteration %d: message lost, queued=(%q,%v)", i, left, leftOK)
		}
		requireQueueAbsent(t, b, "q")
	}
}

func TestTimeoutDeliveryRaceConservesMessage(t *testing.T) {
	const iterations = 10
	for i := range iterations {
		b := newBroker()
		result := asyncGet(context.Background(), b, "q", 20*time.Millisecond)
		waitForWaiters(t, b, "q", 1)
		message := fmt.Sprintf("message-%d", i)
		putDone := make(chan struct{})
		go func() {
			time.Sleep(20 * time.Millisecond)
			b.put("q", message)
			close(putDone)
		}()
		got := receiveResult(t, result)
		select {
		case <-putDone:
		case <-time.After(time.Second):
			t.Fatal("put did not finish")
		}
		left, leftOK := b.get(context.Background(), "q", 0)
		if got.ok {
			if got.message != message || leftOK {
				t.Fatalf("iteration %d: delivered=%#v, queued=(%q,%v)", i, got, left, leftOK)
			}
		} else if !leftOK || left != message {
			t.Fatalf("iteration %d: message lost, queued=(%q,%v)", i, left, leftOK)
		}
		requireQueueAbsent(t, b, "q")
	}
}
