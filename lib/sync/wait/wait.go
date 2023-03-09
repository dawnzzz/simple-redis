package wait

import (
	"sync"
	"time"
)

// Wait a sync.WaitGroup with timeout
type Wait struct {
	wg sync.WaitGroup
}

func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

func (w *Wait) Done() {
	w.wg.Done()
}

func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout blocks until the WaitGroup counter is zero or timeout
// returns true if timeout
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		w.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
		return false // finish normally
	case <-time.After(timeout):
		return true // timeout
	}
}
