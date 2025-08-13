package pebblesql

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
)

const (
	// PebbleHoldIndexTag is the struct tag used to define a field as indexable
	PebbleHoldIndexTag = "pebbleholdIndex"

	// PebbleholdKeyTag is the struct tag used to define a field as a key for use in a Find query
	PebbleholdKeyTag = "pebbleholdKey"

	// pebbleholdPrefixTag is the prefix for an alternate (more standard) version of a struct tag
	pebbleholdPrefixTag         = "pebblehold"
	pebbleholdPrefixIndexValue  = "index"
	pebbleholdPrefixKeyValue    = "key"
	pebbleholdPrefixUniqueValue = "unique"
)

// Store is a wrapper around a Pebble DB
type Store struct {
	db                *pebble.DB
	sequenceBandwidth uint64
	sequences         *sync.Map // map[string]*seqAlloc

	encode EncodeFunc
	decode DecodeFunc
}

// Options allows you set different options from the defaults
// For example the encoding and decoding funcs which default to Gob
type Options struct {
	Encoder           EncodeFunc
	Decoder           DecodeFunc
	DirName           string
	SequenceBandwidth uint64
	PebbleOptions     *pebble.Options
}

// Open opens or creates a pebblesql store.
func Open(options Options) (*Store, error) {
	db, err := pebble.Open(options.DirName, options.PebbleOptions)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:                db,
		sequenceBandwidth: options.SequenceBandwidth,
		sequences:         &sync.Map{},

		encode: options.Encoder,
		decode: options.Decoder,
	}, nil
}

// Pebble returns the underlying Pebble DB
func (s *Store) Pebble() *pebble.DB {
	return s.db
}

// Close closes the pebble db
func (s *Store) Close() error {
	if s.db == nil {
		return fmt.Errorf("already closed")
	}
	err := s.db.Close()
	if err != nil {
		return err
	}
	s.db = nil
	return nil
}

// Storer is the Interface to implement to skip reflect calls on all data passed into the store
type Storer interface {
	Type() string              // used as the index prefix
	Indexes() map[string]Index //[indexname]indexFunc
}

// anonType is created from a reflection of an unknown interface
type anonStorer struct {
	rType   reflect.Type
	indexes map[string]Index
}

// Type returns the name of the type as determined from the reflect package
func (t *anonStorer) Type() string {
	return t.rType.Name()
}

// Indexes returns the Indexes determined by the reflect package on this type
func (t *anonStorer) Indexes() map[string]Index {
	return t.indexes
}

// newStorer creates a type which satisfies the Storer interface based on reflection of the passed in dataType
// if the Type doesn't meet the requirements of a Storer (i.e. doesn't have a name) it panics
// You can avoid any reflection costs, by implementing the Storer interface on a type
func (s *Store) newStorer(dataType interface{}) Storer {
	if storer, ok := dataType.(Storer); ok {
		return storer
	}

	tp := reflect.TypeOf(dataType)
	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	storer := &anonStorer{
		rType:   tp,
		indexes: make(map[string]Index),
	}

	if storer.rType.Name() == "" {
		panic("Invalid Type for Storer. Type is unnamed")
	}
	if storer.rType.Kind() != reflect.Struct {
		panic("Invalid Type for Storer. PebbleHold only works with structs")
	}

	for i := 0; i < storer.rType.NumField(); i++ {
		indexName := ""
		unique := false

		// legacy style: `pebbleholdIndex:"..."` (value ignored; we use field name canonically)
		if strings.Contains(string(storer.rType.Field(i).Tag), PebbleHoldIndexTag) {
			indexName = storer.rType.Field(i).Tag.Get(PebbleHoldIndexTag)
			// store canonically as field name
			indexName = storer.rType.Field(i).Name
		} else if tag := storer.rType.Field(i).Tag.Get(pebbleholdPrefixTag); tag != "" {
			if tag == pebbleholdPrefixIndexValue {
				indexName = storer.rType.Field(i).Name
			} else if tag == pebbleholdPrefixUniqueValue {
				indexName = storer.rType.Field(i).Name
				unique = true
			}
		}

		if indexName != "" {
			field := storer.rType.Field(i).Name
			storer.indexes[indexName] = Index{
				IndexFunc: func(name string, value interface{}) ([]byte, error) {
					v := reflect.ValueOf(value)
					for v.Kind() == reflect.Ptr {
						v = v.Elem()
					}
					return s.encode(v.FieldByName(field).Interface())
				},
				Unique: unique,
			}
		}
	}

	return storer
}

// ---- Sequence allocator on Pebble ----

type seqAlloc struct {
	mu  sync.Mutex
	cur uint64 // next value to hand out
	end uint64 // exclusive
}

// getSequence returns the next sequence value for given type name,
// allocating a new range of size sequenceBandwidth from Pebble atomically when exhausted.
func (s *Store) getSequence(typeName string) (uint64, error) {
	bw := s.sequenceBandwidth
	if bw == 0 {
		bw = 1
	}

	allocIface, _ := s.sequences.LoadOrStore(typeName, &seqAlloc{})
	alloc := allocIface.(*seqAlloc)

	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if alloc.cur < alloc.end {
		v := alloc.cur
		alloc.cur++
		return v, nil
	}

	// Need to allocate a new range from Pebble
	// seq key: "seq:<typeName>"
	seqKey := []byte("seq:" + typeName)

	// Read current value
	var current uint64
	val, closer, err := s.db.Get(seqKey)
	if err == nil {
		if len(val) == 8 {
			current = binary.BigEndian.Uint64(val)
		}
		_ = closer.Close()
	} else if err != pebble.ErrNotFound {
		return 0, err
	}

	// Reserve [current+1, current+bw]
	newEnd := current + bw
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, newEnd)

	b := s.db.NewBatch()
	// overwrite last reserved value
	if err := b.Set(seqKey, buf, nil); err != nil {
		_ = b.Close()
		return 0, err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return 0, err
	}

	alloc.cur = current + 1
	alloc.end = newEnd + 1 // exclusive
	v := alloc.cur
	alloc.cur++
	return v, nil
}

// ---- helpers ----

func typePrefix(typeName string) []byte {
	return []byte("ph_" + typeName + ":")
}

func getKeyField(tp reflect.Type) (reflect.StructField, bool) {
	for i := 0; i < tp.NumField(); i++ {
		if strings.HasPrefix(string(tp.Field(i).Tag), PebbleholdKeyTag) {
			return tp.Field(i), true
		}
		if tag := tp.Field(i).Tag.Get(pebbleholdPrefixTag); tag == pebbleholdPrefixKeyValue {
			return tp.Field(i), true
		}
	}
	return reflect.StructField{}, false
}

func newElemType(datatype interface{}) interface{} {
	tp := reflect.TypeOf(datatype)
	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	return reflect.New(tp).Interface()
}

// makes sure that interface your working with is not a pointer
func getElem(value interface{}) interface{} {
	for reflect.TypeOf(value).Kind() == reflect.Ptr {
		value = reflect.ValueOf(value).Elem().Interface()
	}
	return value
}
