package main

import (
	"container/list"
	"context"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

type queue struct {
	messages []string
	waiters  list.List
}

type waiterState uint8

const (
	waiterWaiting waiterState = iota
	waiterDelivered
	waiterCanceled
)

type waiter struct {
	ch      chan string
	done    <-chan struct{}
	element *list.Element
	state   waiterState
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) put(name, message string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.queues[name]
	if q == nil {
		q = &queue{}
		b.queues[name] = q
	}

	for q.waiters.Len() > 0 {
		element := q.waiters.Front()
		w := element.Value.(*waiter)

		select {
		case <-w.done:
			q.waiters.Remove(element)
			w.element = nil
			w.state = waiterCanceled
			continue
		default:
		}

		q.waiters.Remove(element)
		w.element = nil
		w.state = waiterDelivered
		w.ch <- message
		b.deleteIfEmpty(name, q)
		return
	}

	q.messages = append(q.messages, message)
}

func (b *broker) get(ctx context.Context, name string, timeout time.Duration) (string, bool) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	b.mu.Lock()
	if ctx.Err() != nil {
		b.mu.Unlock()
		return "", false
	}

	q := b.queues[name]

	if q != nil && len(q.messages) > 0 {
		message := q.messages[0]
		q.messages[0] = ""
		q.messages = q.messages[1:]
		b.deleteIfEmpty(name, q)
		b.mu.Unlock()
		return message, true
	}

	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}

	if q == nil {
		q = &queue{}
		b.queues[name] = q
	}
	w := &waiter{
		ch:    make(chan string, 1),
		done:  ctx.Done(),
		state: waiterWaiting,
	}
	w.element = q.waiters.PushBack(w)
	b.mu.Unlock()

	select {
	case message := <-w.ch:
		return message, true
	case <-ctx.Done():
		b.mu.Lock()
		if w.state == waiterWaiting {
			q.waiters.Remove(w.element)
			w.element = nil
			w.state = waiterCanceled
			b.deleteIfEmpty(name, q)
		}
		delivered := w.state == waiterDelivered
		b.mu.Unlock()
		if delivered {
			return <-w.ch, true
		}
		return "", false
	}
}

func (b *broker) deleteIfEmpty(name string, q *queue) {
	if b.queues[name] == q && len(q.messages) == 0 && q.waiters.Len() == 0 {
		delete(b.queues, name)
	}
}

type handler struct {
	broker *broker
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		values, ok := query["v"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		h.broker.put(name, values[0])
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		w.Header().Set("Cache-Control", "no-store")
		timeout, err := requestTimeout(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		message, ok := h.broker.get(r.Context(), name, timeout)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, message)

	default:
		w.Header().Set("Allow", "GET, PUT")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func requestTimeout(r *http.Request) (time.Duration, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return 0, err
	}
	values, ok := query["timeout"]
	if !ok {
		return 0, nil
	}
	if len(values) != 1 {
		return 0, strconv.ErrSyntax
	}
	if _, err := strconv.ParseUint(values[0], 10, 64); err != nil {
		return 0, err
	}
	return time.ParseDuration(values[0] + "s")
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: queue-broker <port>")
	}
	port, err := strconv.Atoi(os.Args[1])
	if err != nil || port < 1 || port > 65535 {
		log.Fatal("invalid port")
	}

	if err := http.ListenAndServe(":"+strconv.Itoa(port), handler{broker: newBroker()}); err != nil {
		log.Fatal(err)
	}
}
