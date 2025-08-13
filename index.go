package pebblesql

import (
	"bytes"
	"reflect"
	"sort"

	"github.com/cockroachdb/pebble"
)

const indexPrefix = "_Index"

// size of iterator keys stored in memory before more are fetched
const iteratorKeyMinCacheSize = 100

// Index is a function that returns the indexable, encoded bytes of the passed in value
type Index struct {
	IndexFunc func(name string, value interface{}) ([]byte, error)
	Unique    bool
}

// --------------------------
// Index write path (Pebble)
// --------------------------

// adds an item to the index
func (s *Store) indexAdd(storer Storer, b *pebble.Batch, key []byte, data interface{}) error {
	indexes := storer.Indexes()
	for name, index := range indexes {
		if err := s.indexUpdate(storer.Type(), name, index, b, key, data, false); err != nil {
			return err
		}
	}
	return nil
}

// removes an item from the index (pass the original/old record)
func (s *Store) indexDelete(storer Storer, b *pebble.Batch, key []byte, originalData interface{}) error {
	indexes := storer.Indexes()
	for name, index := range indexes {
		if err := s.indexUpdate(storer.Type(), name, index, b, key, originalData, true); err != nil {
			return err
		}
	}
	return nil
}

// adds or removes a specific index on an item
func (s *Store) indexUpdate(
	typeName, indexName string,
	index Index,
	b *pebble.Batch,
	key []byte,
	value interface{},
	del bool,
) error {
	indexKey, err := index.IndexFunc(indexName, value)
	if err != nil {
		return err
	}
	if indexKey == nil {
		return nil
	}

	indexKey = append(indexKeyPrefix(typeName, indexName), indexKey...)

	// load current KeyList (if any)
	var kl KeyList
	v, closer, err := s.db.Get(indexKey)
	switch err {
	case nil:
		if decodeErr := s.decode(v, &kl); decodeErr != nil {
			_ = closer.Close()
			return decodeErr
		}
		_ = closer.Close()
	case pebble.ErrNotFound:
		kl = make(KeyList, 0)
	default:
		return err
	}

	if index.Unique && !del && kl.in(key) == false && len(kl) > 0 {
		// unique 위반 (이미 다른 키가 존재)
		return ErrUniqueExists
	}

	if del {
		kl.remove(key)
	} else {
		kl.add(key)
	}

	if len(kl) == 0 {
		return b.Delete(indexKey, nil)
	}

	iVal, err := s.encode(kl)
	if err != nil {
		return err
	}
	return b.Set(indexKey, iVal, nil)
}

// indexKeyPrefix returns the prefix of the pebble key where this index is stored
func indexKeyPrefix(typeName, indexName string) []byte {
	return []byte(indexPrefix + ":" + typeName + ":" + indexName + ":")
}

// newIndexKey returns the pebble key where this index is stored
func newIndexKey(typeName, indexName string, value []byte) []byte {
	return append(indexKeyPrefix(typeName, indexName), value...)
}

// KeyList is a slice of unique, sorted keys([]byte) such as what an index points to
type KeyList [][]byte

func (v *KeyList) add(key []byte) {
	i := sort.Search(len(*v), func(i int) bool {
		return bytes.Compare((*v)[i], key) >= 0
	})
	if i < len(*v) && bytes.Equal((*v)[i], key) {
		return
	}
	*v = append(*v, nil)
	copy((*v)[i+1:], (*v)[i:])
	(*v)[i] = append([]byte(nil), key...) // ensure copy
}

func (v *KeyList) remove(key []byte) {
	i := sort.Search(len(*v), func(i int) bool {
		return bytes.Compare((*v)[i], key) >= 0
	})
	if i < len(*v) && bytes.Equal((*v)[i], key) {
		copy((*v)[i:], (*v)[i+1:])
		(*v)[len(*v)-1] = nil
		*v = (*v)[:len(*v)-1]
	}
}

func (v *KeyList) in(key []byte) bool {
	i := sort.Search(len(*v), func(i int) bool {
		return bytes.Compare((*v)[i], key) >= 0
	})
	return i < len(*v) && bytes.Equal((*v)[i], key)
}

// ----------------------------------
// Iterator / prefix scan (Pebble)
// ----------------------------------

func indexExists(iter *pebble.Iterator, typeName, indexName string) bool {
	iPrefix := indexKeyPrefix(typeName, indexName)

	// any data for the type?
	tPrefix := typePrefix(typeName)
	lb, ub := prefixBounds(tPrefix)
	iter.SetBounds(lb, ub)
	ok := iter.First()
	if !ok {
		// empty dataset for this type → 인덱스 유무를 단정하지 않음 (쿼리 허용)
	}

	// test if an index exists
	lb, ub = prefixBounds(iPrefix)
	iter.SetBounds(lb, ub)
	ok = iter.First()
	return ok
}

type iterator struct {
	keyCache [][]byte

	// fetch next keys chunk
	nextKeys func(*pebble.Iterator) ([][]byte, error)

	iter     *pebble.Iterator
	bookmark *iterBookmark
	lastSeek []byte

	snap *pebble.Snapshot // reads happen against snapshot
	db   *pebble.DB

	err error
}

// iterBookmark stores a seek location in a specific iterator
// so that a single RW iterator can be shared within a single transaction
type iterBookmark struct {
	iter    *pebble.Iterator
	seekKey []byte
}

func (s *Store) newIterator(snap *pebble.Snapshot, typeName string, query *Query, bookmark *iterBookmark) *iterator {
	i := &iterator{
		snap: snap,
		db:   s.db,
	}

	if bookmark != nil {
		i.iter = bookmark.iter
	} else {
		var err error
		i.iter, err = snap.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil
		}
	}

	var prefix []byte

	if query.index != "" {
		query.badIndex = !indexExists(i.iter, typeName, query.index)
	}

	criteria := query.fieldCriteria[query.index]
	if hasMatchFunc(criteria) {
		// 인덱스 못 씀 (전체 레코드 필요)
		criteria = nil
	}

	if query.index == "" || len(criteria) == 0 {
		prefix = typePrefix(typeName)
		lb, ub := prefixBounds(prefix)
		i.iter.SetBounds(lb, ub)
		_ = i.iter.First()

		i.nextKeys = func(iter *pebble.Iterator) ([][]byte, error) {
			var nKeys [][]byte
			for len(nKeys) < iteratorKeyMinCacheSize {
				if !iter.Valid() {
					return nKeys, nil
				}
				key := append([]byte(nil), iter.Key()...)

				ok := true
				if len(criteria) != 0 {
					valBytes := append([]byte(nil), iter.Value()...)
					val := reflect.New(query.dataType)
					if err := s.decode(valBytes, val.Interface()); err != nil {
						return nil, err
					}
					var err error
					ok, err = s.matchesAllCriteria(criteria, key, true, typeName, val.Interface())
					if err != nil {
						return nil, err
					}
				}

				if ok {
					nKeys = append(nKeys, key)
				}
				i.lastSeek = key
				iter.Next()
			}
			return nKeys, nil
		}
		return i
	}

	// (2) index 경로: 인덱스 엔트리에서 KeyList를 풀어 실제 키 목록을 모음
	prefix = indexKeyPrefix(typeName, query.index)
	lb, ub := prefixBounds(prefix)
	i.iter.SetBounds(lb, ub)
	_ = i.iter.First()

	i.nextKeys = func(iter *pebble.Iterator) ([][]byte, error) {
		var nKeys [][]byte
		for len(nKeys) < iteratorKeyMinCacheSize {
			if !iter.Valid() {
				return nKeys, nil
			}

			key := append([]byte(nil), iter.Key()...)
			// 인덱스 키의 suffix(value 부분)를 criteria 매칭에 사용
			ok, err := s.matchesAllCriteria(criteria, key[len(prefix):], true, "", nil)
			if err != nil {
				return nil, err
			}

			if ok {
				// 인덱스 값은 KeyList 직렬화
				val := iter.Value()
				var keys KeyList
				if err := s.decode(val, &keys); err != nil {
					return nil, err
				}
				// append the slice of keys stored in the index
				for _, k := range keys {
					nKeys = append(nKeys, append([]byte(nil), k...))
				}
			}

			i.lastSeek = key
			iter.Next()
		}
		return nKeys, nil
	}

	return i
}

func (i *iterator) createBookmark() *iterBookmark {
	return &iterBookmark{
		iter:    i.iter,
		seekKey: i.lastSeek,
	}
}

// Next returns the next key/value that matches iterator criteria.
// nil,nil when exhausted; on error, returns nil,nil and Error() is set.
func (i *iterator) Next() (key []byte, value []byte) {
	if i.err != nil {
		return nil, nil
	}
	if len(i.keyCache) == 0 {
		newKeys, err := i.nextKeys(i.iter)
		if err != nil {
			i.err = err
			return nil, nil
		}
		if len(newKeys) == 0 {
			return nil, nil
		}
		i.keyCache = append(i.keyCache, newKeys...)
	}

	key = i.keyCache[0]
	i.keyCache = i.keyCache[1:]

	// 읽기는 snapshot에서
	val, closer, err := i.snap.Get(key)
	if err != nil {
		i.err = err
		return nil, nil
	}
	value = append([]byte(nil), val...)
	_ = closer.Close()
	return
}

// Error returns the last error
func (i *iterator) Error() error {
	return i.err
}

func (i *iterator) Close() {
	if i.bookmark != nil {
		i.iter.SeekGE(i.bookmark.seekKey)
		return
	}
	i.iter.Close()
	if i.snap != nil {
		_ = i.snap.Close()
	}
}

// ---------------------
// helper: prefix bounds
// ---------------------

// prefixBounds returns [lower, upper) bounds to iterate keys with the given prefix.
func prefixBounds(prefix []byte) (lower, upper []byte) {
	lower = append([]byte(nil), prefix...)
	upper = make([]byte, len(prefix))
	copy(upper, prefix)
	// increment last byte with carry to get exclusive upper bound
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			upper = upper[:i+1]
			return
		}
	}
	// overflow: no finite upper; let upper be nil to mean "no bound"
	return lower, nil
}
