package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn     *MvccTxn
	iter    engine_util.DBIterator
	nextKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	scanner := &Scanner{
		txn:     txn,
		iter:    iter,
		nextKey: startKey,
	}
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for {
		if scan.nextKey == nil {
			return nil, nil, nil
		}

		// Seek to the first write record for nextKey with ts <= StartTS
		scan.iter.Seek(EncodeKey(scan.nextKey, scan.txn.StartTS))
		if !scan.iter.Valid() {
			scan.nextKey = nil
			return nil, nil, nil
		}

		item := scan.iter.Item()
		userKey := DecodeUserKey(item.Key())

		// If we've moved past the key we're looking for, update nextKey and continue
		if !bytes.Equal(userKey, scan.nextKey) {
			scan.nextKey = userKey
			continue
		}

		// Found a write record for this key, parse it
		value, err := item.Value()
		if err != nil {
			return nil, nil, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, nil, err
		}

		// Move to the next key for the next call
		scan.iter.Next()
		if scan.iter.Valid() {
			nextUserKey := DecodeUserKey(scan.iter.Item().Key())
			if bytes.Equal(nextUserKey, userKey) {
				// Still on the same key, need to skip to next different key
				scan.nextKey = append(userKey, 0)
			} else {
				scan.nextKey = nextUserKey
			}
		} else {
			scan.nextKey = nil
		}

		// If this is a delete, skip this key
		if write.Kind == WriteKindDelete {
			continue
		}

		// If this is a put, get the value from the default CF
		if write.Kind == WriteKindPut {
			val, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
			if err != nil {
				return nil, nil, err
			}
			return userKey, val, nil
		}

		// For rollback, skip
		continue
	}
}
