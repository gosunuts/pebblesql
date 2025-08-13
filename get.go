package pebblesql

import (
	"errors"
	"reflect"

	"github.com/cockroachdb/pebble"
)

// ErrNotFound is returned when no data is found for the given key.
var ErrNotFound = errors.New("no data found for this key")

// Get retrieves a value and decodes it into `result`. `result` must be a pointer.
// Snapshot lifecycle is managed here (top-level), not in helpers.
func (s *Store) Get(key, result interface{}) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	return s.TxGet(snap, key, result)
}

// TxGet retrieves a value using the provided snapshot and decodes it into `result`.
func (s *Store) TxGet(snap *pebble.Snapshot, key, result interface{}) error {
	storer := s.newStorer(result)

	gk, err := s.encodeKey(key, storer.Type())
	if err != nil {
		return err
	}

	val, closer, err := snap.Get(gk)
	if err == pebble.ErrNotFound {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	defer closer.Close()

	if err := s.decode(val, result); err != nil {
		return err
	}

	// populate key field if present
	tp := reflect.TypeOf(result)
	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	if keyField, ok := getKeyField(tp); ok {
		if err := s.decodeKey(
			gk,
			reflect.ValueOf(result).Elem().FieldByName(keyField.Name).Addr().Interface(),
			storer.Type(),
		); err != nil {
			return err
		}
	}

	return nil
}

// Find retrieves a set of values matching `query` and appends them into `result` (pointer to slice).
// Existing contents of the slice are preserved and new results are appended.
func (s *Store) Find(result interface{}, query *Query) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	if query != nil {
		query.snap = snap // make available to subqueries
	}
	return s.TxFind(snap, result, query)
}

// TxFind retrieves matching values using the provided snapshot.
func (s *Store) TxFind(snap *pebble.Snapshot, result interface{}, query *Query) error {
	return s.findQuery(snap, result, query)
}

// FindOne retrieves a single record into `result` (pointer to struct). Returns ErrNotFound if none match.
func (s *Store) FindOne(result interface{}, query *Query) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	if query != nil {
		query.snap = snap // make available to subqueries
	}
	return s.TxFindOne(snap, result, query)
}

// TxFindOne retrieves a single matching record using the provided snapshot.
func (s *Store) TxFindOne(snap *pebble.Snapshot, result interface{}, query *Query) error {
	return s.findOneQuery(snap, result, query)
}

// Count returns the number of records of the given datatype matching `query`.
func (s *Store) Count(dataType interface{}, query *Query) (uint64, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	if query != nil {
		query.snap = snap // keep behavior consistent for any sub-evals
	}
	return s.TxCount(snap, dataType, query)
}

// TxCount returns the number of records using the provided snapshot.
func (s *Store) TxCount(snap *pebble.Snapshot, dataType interface{}, query *Query) (uint64, error) {
	return s.countQuery(snap, dataType, query)
}

// ForEach runs the function `fn` for every record that matches `query`.
// Useful for large datasets to stream results without holding them all in memory.
// Returning a non-nil error from `fn` stops iteration and returns that error.
func (s *Store) ForEach(query *Query, fn interface{}) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	if query != nil {
		query.snap = snap // allow subqueries inside fn
	}
	return s.TxForEach(snap, query, fn)
}

// TxForEach is the same as ForEach but uses the provided snapshot.
func (s *Store) TxForEach(snap *pebble.Snapshot, query *Query, fn interface{}) error {
	return s.forEach(snap, query, fn)
}
