package pebblesql

import (
	"github.com/cockroachdb/pebble"
)

// Delete removes a single record by key.
// It uses a snapshot for read consistency and a batch for atomic mutation.
func (s *Store) Delete(key, dataType interface{}) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	b := s.db.NewBatch()
	defer b.Close()

	if err := s.TxDelete(b, snap, key, dataType); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// TxDelete removes a single record using the provided batch (for writes) and snapshot (for reads).
func (s *Store) TxDelete(b *pebble.Batch, snap *pebble.Snapshot, key, dataType interface{}) error {
	storer := s.newStorer(dataType)

	// Encode logical key (includes type prefix)
	gk, err := s.encodeKey(key, storer.Type())
	if err != nil {
		return err
	}

	// Load existing value via snapshot
	val, closer, err := snap.Get(gk)
	if err == pebble.ErrNotFound {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	defer closer.Close()

	// Decode for index cleanup
	value := newElemType(dataType)
	if err := s.decode(val, value); err != nil {
		return err
	}

	// Delete primary record
	if err := b.Delete(gk, nil); err != nil {
		return err
	}

	// Remove secondary indexes
	return s.indexDelete(storer, b, gk, value)
}

// DeleteMatching removes all records that match the given query.
// It internally builds the set by scanning with a snapshot, then deletes via a batch.
func (s *Store) DeleteMatching(dataType interface{}, query *Query) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	b := s.db.NewBatch()
	defer b.Close()

	if err := s.TxDeleteMatching(b, snap, dataType, query); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// TxDeleteMatching is the same as DeleteMatching but uses the provided batch and snapshot.
func (s *Store) TxDeleteMatching(b *pebble.Batch, snap *pebble.Snapshot, dataType interface{}, query *Query) error {
	return s.deleteQuery(b, snap, dataType, query)
}
