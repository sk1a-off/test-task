package main

import (
	"container/list"
	"context"
	"io"
	"log"
	"net/http"
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

type waiter struct {
	ch        chan string
	element   *list.Element
	delivered bool
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

	if element := q.waiters.Front(); element != nil {
		q.waiters.Remove(element)
		w := element.Value.(*waiter)
		w.delivered = true
		w.ch <- message
		if len(q.messages) == 0 && q.waiters.Len() == 0 {
			delete(b.queues, name)
		}
		return
	}

	q.messages = append(q.messages, message)
}

func (b *broker) get(ctx context.Context, name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.queues[name]

	if q != nil && len(q.messages) > 0 {
		message := q.messages[0]
		q.messages[0] = ""
		q.messages = q.messages[1:]
		if len(q.messages) == 0 && q.waiters.Len() == 0 {
			delete(b.queues, name)
		}
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
	w := &waiter{ch: make(chan string, 1)}
	w.element = q.waiters.PushBack(w)
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case message := <-w.ch:
		return message, true
	case <-ctx.Done():
		b.mu.Lock()
		if !w.delivered {
			q.waiters.Remove(w.element)
			if len(q.messages) == 0 && q.waiters.Len() == 0 {
				delete(b.queues, name)
			}
			b.mu.Unlock()
			return "", false
		}
		b.mu.Unlock()
		return <-w.ch, true
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
		values, ok := r.URL.Query()["v"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		h.broker.put(name, values[0])
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
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
	values, ok := r.URL.Query()["timeout"]
	if !ok {
		return 0, nil
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
