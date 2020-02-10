// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package lockwaiter

import (
	"container/list"
	"sync"
	"time"

	"github.com/ngaut/log"
)

// ErrDeadlock is returned when deadlock is detected.
type ErrDeadlock struct {
	LockKey         []byte
	LockTS          uint64
	DeadlockKeyHash uint64
}

func (e ErrDeadlock) Error() string {
	return "deadlock"
}

// detector detects deadlock.
type Detector struct {
	waitForMap       map[uint64]*txnList
	txnAgeMap        map[uint64]int64
	lock             sync.Mutex
	entryTTL         time.Duration
	totalSize        uint64
	lastActiveExpire time.Time
	urgentSize       uint64
	expireInterval   time.Duration
}

type txnList struct {
	//txns []txnKeyHashPair
	txns *list.List
}

type txnKeyHashPair struct {
	txn          uint64
	keyHash      uint64
	registerTime time.Time
}

func (p *txnKeyHashPair) isExpired(ttl time.Duration, nowTime time.Time) bool {
	if p.registerTime.Add(ttl).Before(nowTime) {
		return true
	}
	return false
}

// NewDetector creates a new detector.
func NewDetector(ttl time.Duration, urgentSize uint64, expireInterval time.Duration) *Detector {
	return &Detector{
		waitForMap:       map[uint64]*txnList{},
		txnAgeMap:        map[uint64]int64{},
		entryTTL:         ttl,
		lastActiveExpire: time.Now(),
		urgentSize:       urgentSize,
		expireInterval:   expireInterval,
	}
}

// Detect detects deadlock for the sourceTxn on a locked key.
func (d *Detector) Detect(sourceTxn, waitForTxn, keyHash uint64) *ErrDeadlock {
	nowTime := time.Now()
	defer func() {
		diff := time.Since(nowTime)
		if diff > time.Millisecond*100 {
			log.Errorf("[for debug] Detect uses=%v in ms", diff.Milliseconds())
		}
	}()
	d.lock.Lock()
	defer d.lock.Unlock()
	d.activeExpire(nowTime)
	err := d.doDetect(nowTime, sourceTxn, waitForTxn)
	if err == nil {
		d.register(sourceTxn, waitForTxn, keyHash)
	}
	return err
}

func (d *Detector) doDetect(nowTime time.Time, sourceTxn, waitForTxn uint64) *ErrDeadlock {
	val := d.waitForMap[waitForTxn]
	if val == nil {
		return nil
	}
	var nextVal *list.Element
	for cur := val.txns.Front(); cur != nil; cur = nextVal {
		nextVal = cur.Next()
		keyHashPair := cur.Value.(*txnKeyHashPair)
		// check if this edge is expired
		if keyHashPair.isExpired(d.entryTTL, nowTime) {
			val.txns.Remove(cur)
			d.totalSize--
			continue
		}
		if keyHashPair.txn == sourceTxn {
			return &ErrDeadlock{DeadlockKeyHash: keyHashPair.keyHash}
		}
		if err := d.doDetect(nowTime, sourceTxn, keyHashPair.txn); err != nil {
			return err
		}
	}
	if val.txns.Len() == 0 {
		delete(d.waitForMap, waitForTxn)
	}
	return nil
}

func (d *Detector) GetTxnAge(txnTs uint64) int64 {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.getTxnAge(txnTs)
}

func (d *Detector) getTxnAge(txnTs uint64) int64 {
	if _, ok := d.txnAgeMap[txnTs]; !ok {
		d.txnAgeMap[txnTs] = 0
	}
	return d.txnAgeMap[txnTs]
}

func (d *Detector) updateTxnAge(txnTs uint64, delta int64) {
	oriAge := d.getTxnAge(txnTs)
	newAge := oriAge + delta
	if newAge < 0 {
		newAge = 0
	}
	d.txnAgeMap[txnTs] = newAge
	waitForMap := d.waitForMap[txnTs]
	//log.Infof("[for debug] updateTxnAge s=%v delta=%v newAge=%v", txnTs, delta, newAge)
	if waitForMap != nil {
		for cur := waitForMap.txns.Front(); cur != nil; cur = cur.Next() {
			valuePair := cur.Value.(*txnKeyHashPair)
			d.updateTxnAge(valuePair.txn, newAge+1)
		}
	}
}

func (d *Detector) Register(sourceTxn, waitForTxn, keyHash uint64) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.register(sourceTxn, waitForTxn, keyHash)
}

func (d *Detector) register(sourceTxn, waitForTxn, keyHash uint64) {
	val := d.waitForMap[sourceTxn]
	pair := txnKeyHashPair{txn: waitForTxn, keyHash: keyHash, registerTime: time.Now()}
	if val == nil {
		newList := &txnList{txns: list.New()}
		newList.txns.PushBack(&pair)
		d.waitForMap[sourceTxn] = newList
		d.totalSize++
		d.updateTxnAge(waitForTxn, d.getTxnAge(sourceTxn)+1)
		return
	}
	for cur := val.txns.Front(); cur != nil; cur = cur.Next() {
		valuePair := cur.Value.(*txnKeyHashPair)
		if valuePair.txn == waitForTxn && valuePair.keyHash == keyHash {
			return
		}
	}
	val.txns.PushBack(&pair)
	d.totalSize++
	d.updateTxnAge(waitForTxn, d.getTxnAge(sourceTxn)+1)
}

// CleanUp removes the wait for entry for the transaction.
func (d *Detector) CleanUp(txn uint64) {
	d.lock.Lock()
	if l, ok := d.waitForMap[txn]; ok {
		d.totalSize -= uint64(l.txns.Len())
		var nextVal *list.Element
		for cur := l.txns.Front(); cur != nil; cur = nextVal {
			nextVal = cur.Next()
			valuePair := cur.Value.(*txnKeyHashPair)
			waitForTxn := valuePair.txn
			//log.Infof("[for debug] CleanUp txn=%v update waitoForTxn=%v delta=%v", txn, waitForTxn, -1*(d.getTxnAge(txn) + 1))
			d.updateTxnAge(waitForTxn, -1*(d.getTxnAge(txn)+1))
		}
	}
	delete(d.waitForMap, txn)
	d.lock.Unlock()
}

// CleanUpWaitFor removes a key in the wait for entry for the transaction.
func (d *Detector) CleanUpWaitFor(txn, waitForTxn, keyHash uint64) {
	d.lock.Lock()
	l := d.waitForMap[txn]
	if l != nil {
		var nextVal *list.Element
		for cur := l.txns.Front(); cur != nil; cur = nextVal {
			nextVal = cur.Next()
			valuePair := cur.Value.(*txnKeyHashPair)
			if valuePair.txn == waitForTxn && valuePair.keyHash == keyHash {
				l.txns.Remove(cur)
				d.totalSize--
				break
			}
		}
		if l.txns.Len() == 0 {
			delete(d.waitForMap, txn)
		}
		//log.Infof("[for debug] CleanUpWaitFor txn=%v update waitoForTxn=%v delta=%v", txn, waitForTxn, -1*(d.getTxnAge(txn) + 1))
		d.updateTxnAge(waitForTxn, -1*(d.getTxnAge(txn)+1))
	}
	d.lock.Unlock()

}

// activeExpire removes expired entries, should be called under d.lock protection
func (d *Detector) activeExpire(nowTime time.Time) {
	if nowTime.Sub(d.lastActiveExpire) > d.expireInterval &&
		d.totalSize >= d.urgentSize {
		log.Infof("detector will do activeExpire, current size=%v", d.totalSize)
		for txn, l := range d.waitForMap {
			var nextVal *list.Element
			for cur := l.txns.Front(); cur != nil; cur = nextVal {
				nextVal = cur.Next()
				valuePair := cur.Value.(*txnKeyHashPair)
				if valuePair.isExpired(d.entryTTL, nowTime) {
					l.txns.Remove(cur)
					d.totalSize--
				}
			}
			if l.txns.Len() == 0 {
				delete(d.waitForMap, txn)
			}
		}
		d.lastActiveExpire = nowTime
		log.Infof("detector activeExpire finished, current size=%v", d.totalSize)
	}
}
