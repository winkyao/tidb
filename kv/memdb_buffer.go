// Copyright 2015 PingCAP, Inc.
//
// Copyright 2015 Wenbin Xiao
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

package kv

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	btree "github.com/pingcap/tidb/util/btree"
)

// memDBBuffer implements the MemBuffer interface.
type memDbBuffer struct {
	db              *btree.Tree
	totalEntrySize  int
	entrySizeLimit  int
	bufferLenLimit  uint64
	bufferSizeLimit int
}

type memDbIter struct {
	iter    *btree.Enumerator
	reverse bool
	key     Key
	val     []byte
	valid   bool
}

func btreeComparer(a, b []byte) int {
	return bytes.Compare(a, b)
}

// NewMemDbBuffer creates a new memDbBuffer.
func NewMemDbBuffer(cap int) MemBuffer {
	return &memDbBuffer{
		db:              btree.TreeNew(btreeComparer),
		entrySizeLimit:  TxnEntrySizeLimit,
		bufferLenLimit:  atomic.LoadUint64(&TxnEntryCountLimit),
		bufferSizeLimit: TxnTotalSizeLimit,
	}
}

// Seek creates an Iterator.
func (m *memDbBuffer) Seek(k Key) (Iterator, error) {
	var (
		iter *btree.Enumerator
		err  error
	)
	if k == nil {
		iter, err = m.db.SeekFirst()
	} else {
		iter, _ = m.db.Seek(k)
	}

	i := &memDbIter{iter: iter, reverse: false, valid: true}
	if err != nil {
		if err == io.EOF {
			// no a real error
			i.valid = false
		} else {
			return nil, errors.Trace(err)
		}
	}
	err = i.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return i, nil
}

func (m *memDbBuffer) SetCap(cap int) {

}

func (m *memDbBuffer) SeekReverse(k Key) (Iterator, error) {
	var (
		iter *btree.Enumerator
		err  error
	)
	if k == nil {
		iter, err = m.db.SeekLast()
	} else {
		lastKey, _ := m.db.Last()
		if btreeComparer(k, lastKey) > 0 {
			iter, err = m.db.SeekLast()
		} else {
			iter, _ = m.db.Seek(k)
			_, _, err = iter.Prev()
		}
	}
	i := &memDbIter{iter: iter, reverse: true, valid: true}
	if err != nil {
		if err == io.EOF {
			// no a real error
			i.valid = false
		} else {
			return nil, errors.Trace(err)
		}
	}
	err = i.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return i, nil
}

// Get returns the value associated with key.
func (m *memDbBuffer) Get(k Key) ([]byte, error) {
	v, ok := m.db.Get(k)
	if !ok {
		return nil, ErrNotExist
	}
	return v, nil
}

// Set associates key with value.
func (m *memDbBuffer) Set(k Key, v []byte) error {
	if len(v) == 0 {
		return errors.Trace(ErrCannotSetNilValue)
	}
	entrySize := len(k) + len(v)
	if entrySize > m.entrySizeLimit {
		return ErrEntryTooLarge.Gen("entry too large, size: %d", len(k)+len(v))
	}

	m.db.Set(k, v)
	m.totalEntrySize += entrySize
	if m.Size() > m.bufferSizeLimit {
		return ErrTxnTooLarge.Gen("transaction too large, size:%d", m.Size())
	}
	if m.Len() > int(m.bufferLenLimit) {
		return ErrTxnTooLarge.Gen("transaction too large, len:%d", m.Len())
	}

	return nil
}

// Delete removes the entry from buffer with provided key.
func (m *memDbBuffer) Delete(k Key) error {
	m.db.Set(k, nil)
	return nil
}

// Size returns sum of keys and values length.
func (m *memDbBuffer) Size() int {
	return m.totalEntrySize
}

// Len returns the number of entries in the DB.
func (m *memDbBuffer) Len() int {
	return m.db.Len()
}

// Reset cleanup the MemBuffer.
func (m *memDbBuffer) Reset() {
	m.db.Clear()
}

// Next implements the Iterator Next.
func (i *memDbIter) Next() error {
	if !i.Valid() {
		return nil
	}

	var err error
	if i.reverse {
		i.key, i.val, err = i.iter.Prev()
	} else {
		i.key, i.val, err = i.iter.Next()
	}
	if err == io.EOF {
		i.valid = false
		return nil
	}
	return errors.Trace(err)
}

// Valid implements the Iterator Valid.
func (i *memDbIter) Valid() bool {
	return i.valid && i.iter != nil
}

// Key implements the Iterator Key.
func (i *memDbIter) Key() Key {
	return i.key
}

// Value implements the Iterator Value.
func (i *memDbIter) Value() []byte {
	return i.val
}

// Close Implements the Iterator Close.
func (i *memDbIter) Close() {
	if i.iter != nil {
		i.iter.Close()
	}
}

// WalkMemBuffer iterates all buffered kv pairs in memBuf
func WalkMemBuffer(memBuf MemBuffer, f func(k Key, v []byte) error) error {
	iter, err := memBuf.Seek(nil)
	if err != nil {
		return errors.Trace(err)
	}

	defer iter.Close()
	for iter.Valid() {
		if err = f(iter.Key(), iter.Value()); err != nil {
			return errors.Trace(err)
		}
		err = iter.Next()
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
