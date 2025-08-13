package pebblesql

import (
	"errors"
	"reflect"

	"github.com/cockroachdb/pebble"
)

// ErrKeyExists is returned when inserting a key that already exists.
var ErrKeyExists = errors.New("this key already exists in pebblesql for this type")

// ErrUniqueExists is returned when a unique constraint would be violated.
var ErrUniqueExists = errors.New("this value cannot be written due to the unique constraint on the field")

// sequence indicates the caller wants an auto-incremented key.
type sequence struct{}

// NextSequence requests an auto-incremented (uint64) key for inserts.
// Example: store.Insert(pebblesql.NextSequence(), data)
func NextSequence() interface{} { return sequence{} }

// Insert writes one record using its key (or an auto-incremented key).
// It creates a snapshot for read-consistency and a batch for atomic writes.
func (s *Store) Insert(key, data interface{}) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	b := s.db.NewBatch()
	defer b.Close()

	if err := s.TxInsert(b, snap, key, data); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// TxInsert is the same as Insert but uses the provided batch and snapshot.
// Callers can compose multiple operations in a single batch before Commit.
func (s *Store) TxInsert(b *pebble.Batch, snap *pebble.Snapshot, key, data interface{}) error {
	storer := s.newStorer(data)

	// Expand NextSequence() into a concrete key
	if _, ok := key.(sequence); ok {
		nxt, err := s.getSequence(storer.Type())
		if err != nil {
			return err
		}
		key = nxt
	}

	gk, err := s.encodeKey(key, storer.Type())
	if err != nil {
		return err
	}

	// Existence check via snapshot
	if _, closer, err := snap.Get(gk); err == nil {
		_ = closer.Close()
		return ErrKeyExists
	} else if err != pebble.ErrNotFound {
		return err
	}

	// Encode value
	value, err := s.encode(data)
	if err != nil {
		return err
	}

	// Put primary record
	if err := b.Set(gk, value, nil); err != nil {
		return err
	}

	// Build secondary indexes
	if err := s.indexAdd(storer, b, gk, data); err != nil {
		return err
	}

	// If data is addressable and has a key field, set it from the insert key (when zero)
	dataVal := reflect.Indirect(reflect.ValueOf(data))
	if !dataVal.CanSet() {
		return nil
	}
	if keyField, ok := getKeyField(dataVal.Type()); ok {
		fieldValue := dataVal.FieldByName(keyField.Name)
		keyValue := reflect.ValueOf(key)
		if keyValue.Type() != keyField.Type {
			return nil
		}
		if !fieldValue.CanSet() {
			return nil
		}
		if !reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(keyField.Type).Interface()) {
			return nil
		}
		fieldValue.Set(keyValue)
	}

	return nil
}

// Update overwrites an existing record by key.
// Fails with ErrNotFound if the key does not exist.
func (s *Store) Update(key interface{}, data interface{}) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	b := s.db.NewBatch()
	defer b.Close()

	if err := s.TxUpdate(b, snap, key, data); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// TxUpdate is the same as Update but uses the provided batch and snapshot.
func (s *Store) TxUpdate(b *pebble.Batch, snap *pebble.Snapshot, key interface{}, data interface{}) error {
	storer := s.newStorer(data)

	gk, err := s.encodeKey(key, storer.Type())
	if err != nil {
		return err
	}

	// Load existing value
	existingBytes, closer, err := snap.Get(gk)
	if err != nil {
		return err
	}
	defer closer.Close()

	// Decode existing for index removal
	existingVal := newElemType(data)
	if err := s.decode(existingBytes, existingVal); err != nil {
		return err
	}
	if err := s.indexDelete(storer, b, gk, existingVal); err != nil {
		return err
	}

	// Encode and write new value
	newBytes, err := s.encode(data)
	if err != nil {
		return err
	}
	if err := b.Set(gk, newBytes, nil); err != nil {
		return err
	}

	// Rebuild indexes
	return s.indexAdd(storer, b, gk, data)
}

// Upsert inserts a new record or updates an existing one.
func (s *Store) Upsert(key interface{}, data interface{}) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	b := s.db.NewBatch()
	defer b.Close()

	if err := s.TxUpsert(b, snap, key, data); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// TxUpsert is the same as Upsert but uses the provided batch and snapshot.
func (s *Store) TxUpsert(b *pebble.Batch, snap *pebble.Snapshot, key interface{}, data interface{}) error {
	storer := s.newStorer(data)

	gk, err := s.encodeKey(key, storer.Type())
	if err != nil {
		return err
	}

	// Check if record exists
	existingBytes, closer, err := snap.Get(gk)
	if err == nil {
		// Existing entry: remove old indexes
		defer closer.Close()
		existingVal := newElemType(data)
		if err := s.decode(existingBytes, existingVal); err != nil {
			return err
		}
		if err := s.indexDelete(storer, b, gk, existingVal); err != nil {
			return err
		}
	} else if err != pebble.ErrNotFound {
		return err
	}

	// Write new value
	value, err := s.encode(data)
	if err != nil {
		return err
	}
	if err := b.Set(gk, value, nil); err != nil {
		return err
	}

	// Build indexes
	return s.indexAdd(storer, b, gk, data)
}

// UpdateMatching runs an update function for every record matching a query.
// Note: the record passed to `update` is always a pointer.
func (s *Store) UpdateMatching(dataType interface{}, query *Query, update func(record interface{}) error) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	b := s.db.NewBatch()
	defer b.Close()

	if err := s.TxUpdateMatching(b, snap, dataType, query, update); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// TxUpdateMatching is the same as UpdateMatching but uses the provided batch and snapshot.
func (s *Store) TxUpdateMatching(b *pebble.Batch, snap *pebble.Snapshot, dataType interface{}, query *Query,
	update func(record interface{}) error) error {
	return s.updateQuery(b, snap, dataType, query, update)
}
