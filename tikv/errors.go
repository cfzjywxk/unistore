package tikv

import (
	"fmt"
)

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the lock then retry.
type ErrLocked struct {
	Key      []byte
	Primary  []byte
	StartTS  uint64
	TTL      uint64
	LockType uint8
}

// BuildLockErr generates ErrKeyLocked objects
func BuildLockErr(key []byte, primaryKey []byte, startTS uint64, TTL uint64, lockType uint8) *ErrLocked {
	errLocked := &ErrLocked{
		Key:      key,
		Primary:  primaryKey,
		StartTS:  startTS,
		TTL:      TTL,
		LockType: lockType,
	}
	return errLocked
}

// Error formats the lock to a string.
func (e *ErrLocked) Error() string {
	return fmt.Sprintf("key is locked, key: %q, Type: %v, primary: %q, startTS: %v", e.Key, e.LockType, e.Primary, e.StartTS)
}

// ErrRetryable suggests that client may restart the txn. e.g. write conflict.
type ErrRetryable string

func (e ErrRetryable) Error() string {
	return fmt.Sprintf("retryable: %s", string(e))
}

var (
	ErrLockNotFound    = ErrRetryable("lock not found")
	ErrAlreadyRollback = ErrRetryable("already rollback")
	ErrReplaced        = ErrRetryable("replaced by another transaction")
)

// ErrAlreadyCommitted is returned specially when client tries to rollback a
// committed lock.
type ErrAlreadyCommitted uint64

func (e ErrAlreadyCommitted) Error() string {
	return "txn already committed"
}

type ErrCommitPessimisticLock struct {
	key []byte
}

func (e ErrCommitPessimisticLock) Error() string {
	return fmt.Sprintf("txn commit pessimistic lock directly on key=%v", e.key)
}

type ErrKeyAlreadyExists struct {
	Key []byte
}

func (e ErrKeyAlreadyExists) Error() string {
	return "key already exists"
}

type ErrConflict struct {
	StartTS          uint64
	ConflictTS       uint64
	ConflictCommitTS uint64
	Key              []byte
}

func (e *ErrConflict) Error() string {
	return "write conflict"
}

// ErrCommitExpire is returned when commit key commitTs smaller than lock.MinCommitTs
type ErrCommitExpire struct {
	StartTs     uint64
	CommitTs    uint64
	MinCommitTs uint64
	Key         []byte
}

func (e *ErrCommitExpire) Error() string {
	return "commit expired"
}

// ErrTxnNotFound is returned if the required txn info not found on storage
type ErrTxnNotFound struct {
	StartTS    uint64
	PrimaryKey []byte
}

func (e *ErrTxnNotFound) Error() string {
	return "txn not found"
}
