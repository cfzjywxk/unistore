package lockwaiter

import (
	"sort"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/ngaut/unistore/config"
	"github.com/pingcap/kvproto/pkg/deadlock"
)

const (
	Detect         int = 0
	CleanUpWaitFor int = 1
	CleanUp        int = 2
)

type DetectTask struct {
	taskType    int
	startTS     uint64
	lockTS      uint64
	keyHash     uint64
	timeout     time.Duration
	isFirstLock bool
	waiter      *Waiter
}

type Manager struct {
	mu            sync.Mutex
	waitingQueues map[uint64]*queue
	detector      *Detector
	TaskCh        chan DetectTask
}

func NewManager() *Manager {
	entryTTL := 1 * time.Second
	urgentSize := uint64(100000)
	exipreInterval := 3600 * time.Second
	taskChSize := 1024
	detector := NewDetector(entryTTL, urgentSize, exipreInterval)
	mgr := &Manager{
		waitingQueues: map[uint64]*queue{},
		detector:      detector,
		TaskCh:        make(chan DetectTask, taskChSize),
	}
	workerNums := 2
	for i := 0; i < workerNums; i++ {
		go mgr.runDetector()
	}
	return mgr
}

type queue struct {
	lm      *Manager
	waiters []*Waiter
}

// getReadyWaiters returns the ready waiters array, and left waiter size in this queue,
// it should be used under map lock protection
func (q *queue) getOldestWaiter() (*Waiter, []*Waiter) {
	// make the waiters in start ts order
	sort.Slice(q.waiters, func(i, j int) bool {
		//return q.waiters[i].startTS < q.waiters[j].startTS
		txnAge1 := q.lm.detector.GetTxnAge(q.waiters[i].startTS)
		txnAge2 := q.lm.detector.GetTxnAge(q.waiters[j].startTS)
		return txnAge1 > txnAge2
	})
	oldestWaiter := q.waiters[0]
	remainWaiter := q.waiters[1:]
	// the remain waiters still exist in the wait queue
	q.waiters = remainWaiter
	return oldestWaiter, remainWaiter
}

// removeWaiter removes the correspond waiter from pending array
// it should be used under map lock protection
func (q *queue) removeWaiter(w *Waiter) {
	for i, waiter := range q.waiters {
		if waiter == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
}

type Waiter struct {
	deadlineTime  time.Time
	timer         *time.Timer
	Ch            chan WaitResult
	startTS       uint64
	LockTS        uint64
	KeyHash       uint64
	CommitTs      uint64
	wakeupDelayed bool
}

// WakeupWaitTime is the implementation of variable "wake-up-delay-duration"
type WakeupWaitTime int

type WaitResult struct {
	// WakeupSleepTime, -1 means the wait is already timeout, 0 means the lock will be granted to this waiter
	// others are the wake-up-delay-duration sleep time, in milliseconds
	WakeupSleepTime WakeupWaitTime
	CommitTS        uint64
	DeadlockResp    *deadlock.DeadlockResponse
}

const WaitTimeout WakeupWaitTime = -1
const WakeUpThisWaiter WakeupWaitTime = 0
const WakeupDelayTimeout WakeupWaitTime = 1

func (w *Waiter) Wait() WaitResult {
	for {
		select {
		case <-w.timer.C:
			if w.wakeupDelayed {
				return WaitResult{WakeupSleepTime: WakeupDelayTimeout, CommitTS: w.CommitTs}
			}
			return WaitResult{WakeupSleepTime: WaitTimeout}
		case result := <-w.Ch:
			if result.WakeupSleepTime == WakeupDelayTimeout {
				w.CommitTs = result.CommitTS
				w.wakeupDelayed = true
				delaySleepDuration := time.Duration(config.GetGlobalConf().PessimisticTxn.WakeUpDelayDuration) * time.Millisecond
				if time.Now().Add(delaySleepDuration).Before(w.deadlineTime) {
					if w.timer.Stop() {
						w.timer.Reset(delaySleepDuration)
					}
				}
				continue
			}
			return result
		}
	}
}

func (w *Waiter) DrainCh() {
	for len(w.Ch) > 0 {
		<-w.Ch
	}
}

// convertErrToResp converts `ErrDeadlock` to `DeadlockResponse` proto type
func convertErrToResp(errDeadlock *ErrDeadlock, txnTs, waitForTxnTs, keyHash uint64) *deadlock.DeadlockResponse {
	entry := deadlock.WaitForEntry{}
	entry.Txn = txnTs
	entry.WaitForTxn = waitForTxnTs
	entry.KeyHash = keyHash
	resp := &deadlock.DeadlockResponse{}
	resp.Entry = entry
	resp.DeadlockKeyHash = errDeadlock.DeadlockKeyHash
	return resp
}

func (lw *Manager) DetectorDetect(startTS, lockTS, keyHash uint64, waiter *Waiter) {
	lw.TaskCh <- DetectTask{taskType: Detect, startTS: startTS,
		lockTS: lockTS, keyHash: keyHash, waiter: waiter}
	if len(lw.TaskCh) > 1000 {
		log.Warnf("task queue near full")
	}
}

func (lw *Manager) DetectorCleanupWaitFor(startTS, lockTS, keyHash uint64) {
	lw.TaskCh <- DetectTask{taskType: CleanUpWaitFor, startTS: startTS, lockTS: lockTS, keyHash: keyHash}
	if len(lw.TaskCh) > 1000 {
		log.Warnf("task queue near full")
	}
}

func (lw *Manager) DetectorCleanup(startTS uint64) {
	lw.TaskCh <- DetectTask{taskType: CleanUp, startTS: startTS}
	if len(lw.TaskCh) > 1000 {
		log.Warnf("task queue near full")
	}
}

// runDetector runs background deadlock detector service
func (lw *Manager) runDetector() {
	for {
		task := <-lw.TaskCh
		switch task.taskType {
		case Detect:
			err := lw.detector.Detect(task.startTS, task.lockTS, task.keyHash)
			if err != nil {
				resp := convertErrToResp(err, task.startTS, task.lockTS, task.keyHash)
				task.waiter.Ch <- WaitResult{DeadlockResp: resp}
			}
		case CleanUpWaitFor:
			lw.detector.CleanUpWaitFor(task.startTS, task.lockTS, task.keyHash)
		case CleanUp:
			lw.detector.CleanUp(task.startTS)
		}
	}
}

// Wait waits on a lock until waked by others or timeout.
func (lw *Manager) NewWaiter(startTS, lockTS, keyHash uint64, timeout time.Duration, isFirstLock bool) *Waiter {
	// allocate memory before hold the lock.
	q := new(queue)
	q.lm = lw
	q.waiters = make([]*Waiter, 0, 8)
	waiter := &Waiter{
		deadlineTime: time.Now().Add(timeout),
		timer:        time.NewTimer(timeout),
		Ch:           make(chan WaitResult, 32),
		startTS:      startTS,
		LockTS:       lockTS,
		KeyHash:      keyHash,
	}
	q.waiters = append(q.waiters, waiter)
	lw.mu.Lock()
	if old, ok := lw.waitingQueues[keyHash]; ok {
		old.waiters = append(old.waiters, waiter)
	} else {
		lw.waitingQueues[keyHash] = q
	}
	lw.mu.Unlock()
	lw.DetectorDetect(startTS, lockTS, keyHash, waiter)
	return waiter
}

// WakeUp wakes up waiters that waiting on the transaction.
func (lw *Manager) WakeUp(txn, commitTS uint64, keyHashes []uint64) {
	waiters := make([]*Waiter, 0, 8)
	wakeUpDelayWaiters := make([]*Waiter, 0, 8)
	lw.mu.Lock()
	for _, keyHash := range keyHashes {
		q := lw.waitingQueues[keyHash]
		if q != nil {
			waiter, remainWaiters := q.getOldestWaiter()
			waiters = append(waiters, waiter)
			if len(remainWaiters) == 0 {
				delete(lw.waitingQueues, keyHash)
			} else {
				wakeUpDelayWaiters = append(wakeUpDelayWaiters, remainWaiters...)
			}
		}
	}
	lw.mu.Unlock()

	// wake up waiters
	if len(waiters) > 0 {
		for _, w := range waiters {
			select {
			case w.Ch <- WaitResult{WakeupSleepTime: WakeUpThisWaiter, CommitTS: commitTS}:
			default:
			}
		}
	}
	// wake up delay waiters, this will not remove waiter from queue
	if len(wakeUpDelayWaiters) > 0 {
		for _, w := range wakeUpDelayWaiters {
			w.LockTS = txn
			select {
			case w.Ch <- WaitResult{WakeupSleepTime: WakeupDelayTimeout, CommitTS: commitTS}:
			default:
			}
		}
	}
}

// CleanUp removes a waiter from waitingQueues when wait timeout.
func (lw *Manager) CleanUp(w *Waiter) {
	lw.mu.Lock()
	q := lw.waitingQueues[w.KeyHash]
	if q != nil {
		q.removeWaiter(w)
		if len(q.waiters) == 0 {
			delete(lw.waitingQueues, w.KeyHash)
		}
	}
	lw.mu.Unlock()
	w.DrainCh()
}

// WakeUpDetection wakes up waiters waiting for deadlock detection results
func (lw *Manager) WakeUpForDeadlock(resp *deadlock.DeadlockResponse) {
	var (
		waiter         *Waiter
		waitForKeyHash uint64
	)
	waitForKeyHash = resp.Entry.KeyHash
	lw.mu.Lock()
	q := lw.waitingQueues[waitForKeyHash]
	if q != nil {
		for i, curWaiter := range q.waiters {
			// there should be no duplicated waiters
			if curWaiter.startTS == resp.Entry.Txn && curWaiter.KeyHash == resp.Entry.KeyHash {
				log.Infof("deadlock detection response got for entry=%v", resp.Entry)
				waiter = curWaiter
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		if len(q.waiters) == 0 {
			delete(lw.waitingQueues, waitForKeyHash)
		}
	}
	lw.mu.Unlock()
	if waiter != nil {
		waiter.Ch <- WaitResult{DeadlockResp: resp}
		log.Infof("wakeup txn=%v blocked by txn=%v because of deadlock, keyHash=%v, deadlockKeyHash=%v",
			resp.Entry.Txn, resp.Entry.WaitForTxn, resp.Entry.KeyHash, resp.DeadlockKeyHash)
	}
}
