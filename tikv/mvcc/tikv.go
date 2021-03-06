package mvcc

import (
	"github.com/coocood/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

type WriteType = byte

const (
	WriteTypeLock     WriteType = 'L'
	WriteTypeRollback WriteType = 'R'
	WriteTypeDelete   WriteType = 'D'
	WriteTypePut      WriteType = 'P'
)

type WriteCFValue struct {
	Type     WriteType
	StartTS  uint64
	ShortVal []byte
}

var invalidWriteCFValue = errors.New("invalid write CF value")

func ParseWriteCFValue(data []byte) (wv WriteCFValue, err error) {
	if len(data) == 0 {
		err = invalidWriteCFValue
		return
	}
	wv.Type = data[0]
	switch wv.Type {
	case WriteTypePut, WriteTypeDelete, WriteTypeLock, WriteTypeRollback:
	default:
		err = invalidWriteCFValue
		return
	}
	wv.ShortVal, wv.StartTS, err = codec.DecodeUvarint(data[1:])
	return
}

const (
	shortValuePrefix = 'v'
	forUpdatePrefix  = 'f'
	shortValueMaxLen = 64
)

// EncodeWriteCFValue accepts a write cf parameters and return the encoded bytes data.
// Just like the tikv encoding form. See tikv/src/storage/mvcc/write.rs for more detail.
func EncodeWriteCFValue(t WriteType, startTs uint64, shortVal []byte) []byte {
	data := make([]byte, 1)
	data[0] = byte(t)
	data = codec.EncodeUvarint(data, startTs)
	if len(shortVal) != 0 {
		data = append(data, byte(shortValuePrefix), byte(len(shortVal)))
		return append(data, shortVal...)
	}
	return data
}

// EncodeLockCFValue encodes the mvcc lock and returns putLock value and putDefault value if exists.
func EncodeLockCFValue(lock *MvccLock) ([]byte, []byte) {
	data := make([]byte, 1)
	switch lock.Op {
	case byte(kvrpcpb.Op_Put):
		data[0] = LockTypePut
	case byte(kvrpcpb.Op_Del):
		data[0] = LockTypeDelete
	case byte(kvrpcpb.Op_Lock):
		data[0] = LockTypeLock
	case byte(kvrpcpb.Op_PessimisticLock):
		data[0] = LockTypePessimistic
	default:
		panic("invalid lock op")
	}
	var longValue []byte
	data = codec.EncodeUvarint(codec.EncodeCompactBytes(data, lock.Primary), lock.StartTS)
	data = codec.EncodeUvarint(data, uint64(lock.TTL))
	if len(lock.Value) <= shortValueMaxLen {
		if len(lock.Value) != 0 {
			data = append(data, byte(shortValuePrefix), byte(len(lock.Value)))
			data = append(data, lock.Value...)
		}
	} else {
		longValue = y.SafeCopy(nil, lock.Value)
	}
	if lock.ForUpdateTS > 0 {
		data = append(data, byte(forUpdatePrefix))
		data = codec.EncodeUint(data, lock.ForUpdateTS)
	}
	return data, longValue
}

type LockType = byte

const (
	LockTypePut         LockType = 'P'
	LockTypeDelete      LockType = 'D'
	LockTypeLock        LockType = 'L'
	LockTypePessimistic LockType = 'S'
)

var invalidLockCFValue = errors.New("invalid lock CF value")

func ParseLockCFValue(data []byte) (lock MvccLock, err error) {
	if len(data) == 0 {
		err = invalidLockCFValue
		return
	}
	switch data[0] {
	case LockTypePut:
		lock.Op = byte(kvrpcpb.Op_Put)
	case LockTypeDelete:
		lock.Op = byte(kvrpcpb.Op_Del)
	case LockTypeLock:
		lock.Op = byte(kvrpcpb.Op_Lock)
	case LockTypePessimistic:
		lock.Op = byte(kvrpcpb.Op_PessimisticLock)
	default:
		err = invalidLockCFValue
		return
	}
	data, lock.Primary, err = codec.DecodeCompactBytes(data[1:])
	if err != nil {
		return
	}
	lock.PrimaryLen = uint16(len(lock.Primary))
	data, lock.StartTS, err = codec.DecodeUvarint(data)
	if err != nil || len(data) == 0 {
		return
	}
	var ttl uint64
	data, ttl, err = codec.DecodeUvarint(data)
	lock.TTL = uint32(ttl)
	if err != nil || len(data) == 0 {
		return
	}
	if data[0] == shortValuePrefix {
		shortValLen := int(data[1])
		data = data[2:]
		lock.Value = data[:shortValLen]
		data = data[shortValLen:]
	}
	if len(data) > 0 && data[0] == forUpdatePrefix {
		data, lock.ForUpdateTS, err = codec.DecodeUint(data[1:])
	}
	return
}
