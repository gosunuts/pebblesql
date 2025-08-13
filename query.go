package pebblesql

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/cockroachdb/pebble"
)

const (
	eq    = iota // ==
	ne           // !=
	gt           // >
	lt           // <
	ge           // >=
	le           // <=
	in           // in
	re           // regular expression
	fn           // func
	isnil        // tests for nil
	sw           // string starts with
	ew           // string ends with
	hk           // match map keys

	contains // slice only
	any      // slice only
	all      // slice only
)

// Key is a shorthand for addressing the encoded key in comparisons (Where(Key).Eq(...)).
const Key = ""

// Query is a chained collection of criteria. An empty query matches all records.
type Query struct {
	index         string
	currentField  string
	fieldCriteria map[string][]*Criterion
	ors           []*Query

	badIndex bool
	dataType reflect.Type

	// Pebble-specific execution context
	snap     *pebble.Snapshot // read snapshot used during execution
	writable bool             // true when caller intends to write (delete/update)
	subquery bool
	bookmark *iterBookmark

	limit   int
	skip    int
	sort    []string
	reverse bool
}

// Slice turns any slice into []interface{} by copying elements.
// Panics if value is not a slice/array/map.
func Slice(value interface{}) []interface{} {
	slc := reflect.ValueOf(value)
	s := make([]interface{}, slc.Len())
	for i := range s {
		s[i] = slc.Index(i).Interface()
	}
	return s
}

// IsEmpty returns true if the query has no criteria and no index selection.
func (q *Query) IsEmpty() bool {
	if q.index != "" {
		return false
	}
	if len(q.fieldCriteria) != 0 {
		return false
	}
	if q.ors != nil {
		return false
	}
	return true
}

// Criterion represents one operator and its value(s) for a field.
type Criterion struct {
	query    *Query
	operator int
	value    interface{}
	values   []interface{}
}

func hasMatchFunc(criteria []*Criterion) bool {
	for _, c := range criteria {
		if c.operator == fn {
			return true
		}
	}
	return false
}

// Field is a typed alias for field names in criteria (kept for API parity).
type Field string

// Where starts a query against an exported struct field.
func Where(field string) *Criterion {
	if !startsUpper(field) {
		panic("The first letter of a field in a pebblesql query must be upper-case")
	}
	return &Criterion{
		query: &Query{
			currentField:  field,
			fieldCriteria: make(map[string][]*Criterion),
		},
	}
}

// And continues the query with another field.
func (q *Query) And(field string) *Criterion {
	if !startsUpper(field) {
		panic("The first letter of a field in a pebblesql query must be upper-case")
	}
	q.currentField = field
	return &Criterion{query: q}
}

// Skip sets how many matched records to skip (applied after filtering).
func (q *Query) Skip(amount int) *Query {
	if amount < 0 {
		panic("Skip must be set to a positive number")
	}
	if q.skip != 0 {
		panic(fmt.Sprintf("Skip has already been set to %d", q.skip))
	}
	q.skip = amount
	return q
}

// Limit sets the maximum number of records to return.
func (q *Query) Limit(amount int) *Query {
	if amount < 0 {
		panic("Limit must be set to a positive number")
	}
	if q.limit != 0 {
		panic(fmt.Sprintf("Limit has already been set to %d", q.limit))
	}
	q.limit = amount
	return q
}

// Contains checks whether the current field (slice) contains the value.
func (c *Criterion) Contains(value interface{}) *Query { return c.op(contains, value) }

// ContainsAll checks whether the current field (slice) contains all given values.
func (c *Criterion) ContainsAll(values ...interface{}) *Query {
	c.operator = all
	c.values = values
	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)
	return q
}

// ContainsAny checks whether the current field (slice) contains any of given values.
func (c *Criterion) ContainsAny(values ...interface{}) *Query {
	c.operator = any
	c.values = values
	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)
	return q
}

// HasKey checks whether the current field (map) has the given key.
func (c *Criterion) HasKey(value interface{}) *Query { return c.op(hk, value) }

// SortBy defines the order-by fields. Multiple fields are supported.
func (q *Query) SortBy(fields ...string) *Query {
	for i := range fields {
		if fields[i] == Key {
			panic("Cannot sort by Key.")
		}
		var found bool
		for k := range q.sort {
			if q.sort[k] == fields[i] {
				found = true
				break
			}
		}
		if !found {
			q.sort = append(q.sort, fields[i])
		}
	}
	return q
}

// Reverse toggles result order reversal (applied with SortBy).
func (q *Query) Reverse() *Query {
	q.reverse = !q.reverse
	return q
}

// Index selects a pre-declared index to drive the query.
func (q *Query) Index(indexName string) *Query {
	if strings.Contains(indexName, ".") {
		// NOTE: nested indexes not supported
		panic("Nested indexes are not supported. Only top level structures can be indexed")
	}
	q.index = indexName
	return q
}

// validateIndex checks whether the selected index exists for the given type.
func (q *Query) validateIndex(data interface{}) error {
	if q.index == "" {
		return nil
	}
	if q.dataType == nil {
		panic("Can't check for a valid index before query datatype is set")
	}

	if storer, ok := data.(Storer); ok {
		if _, ok = storer.Indexes()[q.index]; ok {
			return nil
		}
		return fmt.Errorf("The index %s does not exist", q.index)
	}

	if _, ok := q.dataType.FieldByName(q.index); ok {
		return nil
	}

	for i := 0; i < q.dataType.NumField(); i++ {
		if tag := q.dataType.Field(i).Tag.Get(PebbleHoldIndexTag); tag == q.index {
			q.index = q.dataType.Field(i).Name
			return nil
		}
	}
	return fmt.Errorf("The index %s does not exist", q.index)
}

// Or unions the results of another query with this one (no skip/limit allowed on subqueries).
func (q *Query) Or(query *Query) *Query {
	if query.skip != 0 || query.limit != 0 {
		panic("Or'd queries cannot contain skip or limit values")
	}
	q.ors = append(q.ors, query)
	return q
}

// Matches checks whether `data` would match this query (ignores skip/limit/sort).
func (q *Query) Matches(s *Store, data interface{}) (bool, error) {
	var key []byte
	dataVal := reflect.ValueOf(data)
	for dataVal.Kind() == reflect.Ptr {
		dataVal = dataVal.Elem()
	}
	data = dataVal.Interface()
	storer := s.newStorer(data)
	if keyField, ok := getKeyField(dataVal.Type()); ok {
		fieldValue := dataVal.FieldByName(keyField.Name)
		var err error
		key, err = s.encodeKey(fieldValue.Interface(), storer.Type())
		if err != nil {
			return false, err
		}
	}
	return q.matches(s, key, dataVal, data)
}

func (q *Query) matches(s *Store, key []byte, value reflect.Value, data interface{}) (bool, error) {
	if result, err := q.matchesAllFields(s, key, value, data); result || err != nil {
		return result, err
	}
	for _, orQuery := range q.ors {
		if result, err := orQuery.matches(s, key, value, data); result || err != nil {
			return result, err
		}
	}
	return false, nil
}

func (q *Query) matchesAllFields(s *Store, key []byte, value reflect.Value, currentRow interface{}) (bool, error) {
	if q.IsEmpty() {
		return true, nil
	}
	for field, criteria := range q.fieldCriteria {
		if field == q.index && !q.badIndex && !hasMatchFunc(criteria) {
			// Already handled by index-driven iteration.
			continue
		}
		if field == Key {
			ok, err := s.matchesAllCriteria(criteria, key, true, q.dataType.Name(), currentRow)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		fVal, err := fieldValue(value, field)
		if err != nil {
			return false, err
		}
		ok, err := s.matchesAllCriteria(criteria, fVal.Interface(), false, "", currentRow)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func fieldValue(value reflect.Value, field string) (reflect.Value, error) {
	fields := strings.Split(field, ".")
	current := value
	for i := range fields {
		if current.Kind() == reflect.Ptr {
			current = current.Elem().FieldByName(fields[i])
		} else {
			current = current.FieldByName(fields[i])
		}
		if !current.IsValid() {
			return reflect.Value{}, fmt.Errorf("The field %s does not exist in the type %s", field, value)
		}
	}
	return current, nil
}

func (c *Criterion) op(op int, value interface{}) *Query {
	c.operator = op
	c.value = value
	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)
	return q
}

// Comparison helpers.
func (c *Criterion) Eq(v interface{}) *Query { return c.op(eq, v) }
func (c *Criterion) Ne(v interface{}) *Query { return c.op(ne, v) }
func (c *Criterion) Gt(v interface{}) *Query { return c.op(gt, v) }
func (c *Criterion) Lt(v interface{}) *Query { return c.op(lt, v) }
func (c *Criterion) Ge(v interface{}) *Query { return c.op(ge, v) }
func (c *Criterion) Le(v interface{}) *Query { return c.op(le, v) }

func (c *Criterion) In(values ...interface{}) *Query {
	c.operator = in
	c.values = values
	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)
	return q
}

func (c *Criterion) RegExp(expr *regexp.Regexp) *Query { return c.op(re, expr) }
func (c *Criterion) IsNil() *Query                     { return c.op(isnil, nil) }
func (c *Criterion) HasPrefix(prefix string) *Query    { return c.op(sw, prefix) }
func (c *Criterion) HasSuffix(suffix string) *Query    { return c.op(ew, suffix) }

// MatchFunc allows evaluating arbitrary logic against the current field/record.
type MatchFunc func(ra *RecordAccess) (bool, error)

// RecordAccess exposes field/record/subquery hooks to a MatchFunc.
type RecordAccess struct {
	record interface{}
	field  interface{}
	query  *Query
	store  *Store
}

func (r *RecordAccess) Field() interface{}  { return r.field }
func (r *RecordAccess) Record() interface{} { return r.record }

// SubQuery runs another query in the same logical context (reuses iterator bookmark and snapshot).
func (r *RecordAccess) SubQuery(result interface{}, query *Query) error {
	query.subquery = true
	query.bookmark = r.query.bookmark
	return r.store.findQuery(r.query.snap, result, query)
}

// SubAggregateQuery runs an aggregate query in the same logical context.
func (r *RecordAccess) SubAggregateQuery(query *Query, groupBy ...string) ([]*AggregateResult, error) {
	query.subquery = true
	query.bookmark = r.query.bookmark
	return r.store.aggregateQuery(r.query.snap, r.record, query, groupBy...)
}

func (c *Criterion) MatchFunc(match MatchFunc) *Query {
	if c.query.currentField == Key {
		panic("Match func cannot be used against Keys")
	}
	return c.op(fn, match)
}

// test evaluates one criterion against a value (encoded or decoded).
func (c *Criterion) test(s *Store, testValue interface{}, encoded bool, keyType string, currentRow interface{}) (bool, error) {
	var recordValue interface{}
	if encoded {
		if len(testValue.([]byte)) != 0 {
			if c.operator == in || c.operator == any || c.operator == all {
				recordValue = newElemType(c.values[0])
			} else {
				recordValue = newElemType(c.value)
			}
			if keyType != "" {
				if err := s.decodeKey(testValue.([]byte), recordValue, keyType); err != nil {
					return false, err
				}
			} else {
				if err := s.decode(testValue.([]byte), recordValue); err != nil {
					return false, err
				}
			}
		}
	} else {
		recordValue = testValue
	}

	switch c.operator {
	case in:
		for i := range c.values {
			result, err := c.compare(recordValue, c.values[i], currentRow)
			if err != nil {
				return false, err
			}
			if result == 0 {
				return true, nil
			}
		}
		return false, nil
	case re:
		return c.value.(*regexp.Regexp).Match([]byte(fmt.Sprintf("%s", recordValue))), nil
	case hk:
		v := reflect.ValueOf(recordValue).MapIndex(reflect.ValueOf(c.value))
		return !reflect.ValueOf(v).IsZero(), nil
	case fn:
		return c.value.(MatchFunc)(&RecordAccess{
			field:  recordValue,
			record: currentRow,
			query:  c.query,
			store:  s,
		})
	case isnil:
		return reflect.ValueOf(recordValue).IsNil(), nil
	case sw:
		return strings.HasPrefix(fmt.Sprintf("%s", getElem(recordValue)), fmt.Sprintf("%s", c.value)), nil
	case ew:
		return strings.HasSuffix(fmt.Sprintf("%s", getElem(recordValue)), fmt.Sprintf("%s", c.value)), nil
	case contains, any, all:
		slc := reflect.ValueOf(recordValue)
		kind := slc.Kind()
		if kind != reflect.Slice && kind != reflect.Array {
			for slc.Kind() == reflect.Ptr {
				slc = slc.Elem()
			}
			slc = reflect.Append(reflect.MakeSlice(reflect.SliceOf(slc.Type()), 0, 1), slc)
		}
		if c.operator == contains {
			for i := 0; i < slc.Len(); i++ {
				result, err := c.compare(slc.Index(i), c.value, currentRow)
				if err != nil {
					return false, err
				}
				if result == 0 {
					return true, nil
				}
			}
			return false, nil
		}
		if c.operator == any {
			for i := 0; i < slc.Len(); i++ {
				for k := range c.values {
					result, err := c.compare(slc.Index(i), c.values[k], currentRow)
					if err != nil {
						return false, err
					}
					if result == 0 {
						return true, nil
					}
				}
			}
			return false, nil
		}
		// c.operator == all
		for k := range c.values {
			found := false
			for i := 0; i < slc.Len(); i++ {
				result, err := c.compare(slc.Index(i), c.values[k], currentRow)
				if err != nil {
					return false, err
				}
				if result == 0 {
					found = true
					break
				}
			}
			if !found {
				return false, nil
			}
		}
		return true, nil
	default:
		// comparison operators
		result, err := c.compare(recordValue, c.value, currentRow)
		if err != nil {
			return false, err
		}
		switch c.operator {
		case eq:
			return result == 0, nil
		case ne:
			return result != 0, nil
		case gt:
			return result > 0, nil
		case lt:
			return result < 0, nil
		case le:
			return result < 0 || result == 0, nil
		case ge:
			return result > 0 || result == 0, nil
		default:
			panic("invalid operator")
		}
	}
}

func (s *Store) matchesAllCriteria(criteria []*Criterion, value interface{}, encoded bool, keyType string, currentRow interface{}) (bool, error) {
	for i := range criteria {
		ok, err := criteria[i].test(s, value, encoded, keyType, currentRow)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func startsUpper(str string) bool {
	if str == "" {
		return true
	}
	for _, r := range str {
		return unicode.IsUpper(r)
	}
	return false
}

func (q *Query) String() string {
	s := ""
	if q.index != "" {
		s += "Using Index [" + q.index + "] "
	}
	s += "Where "
	for field, criteria := range q.fieldCriteria {
		for i := range criteria {
			s += field + " " + criteria[i].String()
			s += "\n\tAND "
		}
	}
	// trim last AND
	if len(s) >= 6 {
		s = s[:len(s)-6]
	}
	for i := range q.ors {
		s += "\nOr " + q.ors[i].String()
	}
	return s
}

func (c *Criterion) String() string {
	s := ""
	switch c.operator {
	case eq:
		s += "=="
	case ne:
		s += "!="
	case gt:
		s += ">"
	case lt:
		s += "<"
	case le:
		s += "<="
	case ge:
		s += ">="
	case in:
		return "in " + fmt.Sprintf("%v", c.values)
	case re:
		s += "matches the regular expression"
	case fn:
		s += "matches the function"
	case isnil:
		return "is nil"
	case sw:
		return "starts with " + fmt.Sprintf("%+v", c.value)
	case ew:
		return "ends with " + fmt.Sprintf("%+v", c.value)
	default:
		panic("invalid operator")
	}
	return s + " " + fmt.Sprintf("%v", c.value)
}

type record struct {
	key   []byte
	value reflect.Value
}

// runQuery executes the query with optional skip/limit and feeds each matched record to `action`.
// `retrievedKeys` tracks keys already returned (for OR unions).
func (s *Store) runQuery(snap *pebble.Snapshot, dataType interface{}, query *Query, retrievedKeys KeyList, skip int,
	action func(r *record) error) error {

	storer := s.newStorer(dataType)

	tp := dataType
	for reflect.TypeOf(tp).Kind() == reflect.Ptr {
		tp = reflect.ValueOf(tp).Elem().Interface()
	}

	query.dataType = reflect.TypeOf(tp)
	if err := query.validateIndex(dataType); err != nil {
		return err
	}

	if len(query.sort) > 0 {
		return s.runQuerySort(snap, dataType, query, action)
	}

	iter := s.newIterator(snap, storer.Type(), query, query.bookmark)
	if (query.writable || query.subquery) && query.bookmark == nil {
		query.bookmark = iter.createBookmark()
	}

	defer func() {
		iter.Close()
		query.bookmark = nil
	}()

	if query.index != "" && query.badIndex {
		return fmt.Errorf("The index %s does not exist", query.index)
	}

	newKeys := make(KeyList, 0)
	limit := query.limit - len(retrievedKeys)

	for k, v := iter.Next(); k != nil; k, v = iter.Next() {
		if len(retrievedKeys) != 0 && retrievedKeys.in(k) {
			continue
		}

		val := reflect.New(reflect.TypeOf(tp))
		if err := s.decode(v, val.Interface()); err != nil {
			return err
		}

		// expose snapshot for nested subqueries
		query.snap = snap

		ok, err := query.matchesAllFields(s, k, val, val.Interface())
		if err != nil {
			return err
		}

		if ok {
			if skip > 0 {
				skip--
				continue
			}

			if err := action(&record{key: k, value: val}); err != nil {
				return err
			}

			newKeys.add(k)

			if query.limit != 0 {
				limit--
				if limit == 0 {
					break
				}
			}
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	if query.limit != 0 && limit == 0 {
		return nil
	}

	if len(query.ors) > 0 {
		iter.Close()
		for i := range newKeys {
			retrievedKeys.add(newKeys[i])
		}
		for i := range query.ors {
			if err := s.runQuery(snap, tp, query.ors[i], retrievedKeys, skip, action); err != nil {
				return err
			}
		}
	}

	return nil
}

// runQuerySort materializes all rows then applies sort/skip/limit.
func (s *Store) runQuerySort(snap *pebble.Snapshot, dataType interface{}, query *Query, action func(r *record) error) error {
	if err := validateSortFields(query); err != nil {
		return err
	}

	qCopy := *query
	qCopy.sort = nil
	qCopy.limit = 0
	qCopy.skip = 0

	var records []*record
	if err := s.runQuery(snap, dataType, &qCopy, nil, 0, func(r *record) error {
		records = append(records, r)
		return nil
	}); err != nil {
		return err
	}

	sort.Slice(records, func(i, j int) bool {
		return sortFunction(query, records[i].value, records[j].value)
	})

	startIndex, endIndex := getSkipAndLimitRange(query, len(records))
	records = records[startIndex:endIndex]

	for i := range records {
		if err := action(records[i]); err != nil {
			return err
		}
	}
	return nil
}

func getSkipAndLimitRange(query *Query, recordsLen int) (startIndex, endIndex int) {
	if query.skip > recordsLen {
		return 0, 0
	}
	startIndex = query.skip
	endIndex = recordsLen
	limitIndex := query.limit + startIndex
	if query.limit > 0 && limitIndex <= recordsLen {
		endIndex = limitIndex
	}
	return startIndex, endIndex
}

func sortFunction(query *Query, first, second reflect.Value) bool {
	for _, field := range query.sort {
		val, err := fieldValue(reflect.Indirect(first), field)
		if err != nil {
			panic(err.Error())
		}
		value := val.Interface()

		val, err = fieldValue(reflect.Indirect(second), field)
		if err != nil {
			panic(err.Error())
		}
		other := val.Interface()

		if query.reverse {
			value, other = other, value
		}

		cmp, cerr := compare(value, other)
		if cerr != nil {
			// Fallback: lexicographic compare.
			valS := fmt.Sprintf("%s", value)
			otherS := fmt.Sprintf("%s", other)
			if valS < otherS {
				return true
			} else if valS == otherS {
				continue
			}
			return false
		}
		if cmp == -1 {
			return true
		} else if cmp == 0 {
			continue
		}
		return false
	}
	return false
}

func validateSortFields(query *Query) error {
	for _, field := range query.sort {
		fields := strings.Split(field, ".")
		current := query.dataType
		for i := range fields {
			var sf reflect.StructField
			found := false
			if current.Kind() == reflect.Ptr {
				sf, found = current.Elem().FieldByName(fields[i])
			} else {
				sf, found = current.FieldByName(fields[i])
			}
			if !found {
				return fmt.Errorf("The field %s does not exist in the type %s", field, query.dataType)
			}
			current = sf.Type
		}
	}
	return nil
}

// findQuery executes the query and appends results into `result` slice (pointer to slice).
func (s *Store) findQuery(snap *pebble.Snapshot, result interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
	}
	query.writable = false

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	if isFindByIndexQuery(query) {
		return s.findByIndexQuery(snap, resultVal, query)
	}

	sliceVal := resultVal.Elem()
	elType := sliceVal.Type().Elem()

	tp := elType
	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	keyField, hasKeyField := getKeyField(tp)
	val := reflect.New(tp)

	if err := s.runQuery(snap, val.Interface(), query, nil, query.skip,
		func(r *record) error {
			var rowValue reflect.Value
			if elType.Kind() == reflect.Ptr {
				rowValue = r.value
			} else {
				rowValue = r.value.Elem()
			}

			if hasKeyField {
				rowKey := rowValue
				for rowKey.Kind() == reflect.Ptr {
					rowKey = rowKey.Elem()
				}
				if err := s.decodeKey(r.key, rowKey.FieldByName(keyField.Name).Addr().Interface(), tp.Name()); err != nil {
					return err
				}
			}

			sliceVal = reflect.Append(sliceVal, rowValue)
			return nil
		}); err != nil {
		return err
	}

	resultVal.Elem().Set(sliceVal.Slice(0, sliceVal.Len()))
	return nil
}

func isFindByIndexQuery(query *Query) bool {
	if query.index == "" || len(query.fieldCriteria) == 0 || len(query.fieldCriteria[query.index]) != 1 || len(query.ors) > 0 {
		return false
	}
	op := query.fieldCriteria[query.index][0].operator
	return op == eq || op == in
}

// deleteQuery deletes all rows matching the query using a write batch.
func (s *Store) deleteQuery(b *pebble.Batch, snap *pebble.Snapshot, dataType interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
	}
	query.writable = true

	var records []*record
	if err := s.runQuery(snap, dataType, query, nil, query.skip, func(r *record) error {
		records = append(records, r)
		return nil
	}); err != nil {
		return err
	}

	storer := s.newStorer(dataType)
	for i := range records {
		if err := b.Delete(records[i].key, nil); err != nil {
			return err
		}
		// remove any indexes
		if err := s.indexDelete(storer, b, records[i].key, records[i].value.Interface()); err != nil {
			return err
		}
	}
	return nil
}

// updateQuery updates all rows matching the query using a write batch.
func (s *Store) updateQuery(b *pebble.Batch, snap *pebble.Snapshot, dataType interface{}, query *Query, update func(record interface{}) error) error {
	if query == nil {
		query = &Query{}
	}
	query.writable = true

	var records []*record
	if err := s.runQuery(snap, dataType, query, nil, query.skip, func(r *record) error {
		records = append(records, r)
		return nil
	}); err != nil {
		return err
	}

	storer := s.newStorer(dataType)
	for i := range records {
		upVal := records[i].value.Interface()

		// remove existing indexes based on original value
		if err := s.indexDelete(storer, b, records[i].key, upVal); err != nil {
			return err
		}

		if err := update(upVal); err != nil {
			return err
		}

		encVal, err := s.encode(upVal)
		if err != nil {
			return err
		}
		if err := b.Set(records[i].key, encVal, nil); err != nil {
			return err
		}

		// insert new indexes
		if err := s.indexAdd(storer, b, records[i].key, upVal); err != nil {
			return err
		}
	}
	return nil
}

// aggregateQuery groups matched rows by given fields and returns aggregate buckets.
func (s *Store) aggregateQuery(snap *pebble.Snapshot, dataType interface{}, query *Query, groupBy ...string) ([]*AggregateResult, error) {
	if query == nil {
		query = &Query{}
	}
	query.writable = false

	var result []*AggregateResult
	if len(groupBy) == 0 {
		result = append(result, &AggregateResult{})
	}

	if err := s.runQuery(snap, dataType, query, nil, query.skip, func(r *record) error {
		if len(groupBy) == 0 {
			result[0].reduction = append(result[0].reduction, r.value)
			return nil
		}

		grouping := make([]reflect.Value, len(groupBy))
		for i := range groupBy {
			fVal := r.value.Elem().FieldByName(groupBy[i])
			if !fVal.IsValid() {
				return fmt.Errorf("The field %s does not exist in the type %s", groupBy[i], r.value.Type())
			}
			grouping[i] = fVal
		}

		var err error
		var c int
		var allEqual bool
		idx := sort.Search(len(result), func(i int) bool {
			for j := range grouping {
				c, err = compare(result[i].group[j].Interface(), grouping[j].Interface())
				if err != nil {
					return true
				}
				if c != 0 {
					return c >= 0
				}
			}
			allEqual = true
			return true
		})
		if err != nil {
			return err
		}

		if idx < len(result) && allEqual {
			result[idx].reduction = append(result[idx].reduction, r.value)
			return nil
		}

		result = append(result, nil)
		copy(result[idx+1:], result[idx:])
		result[idx] = &AggregateResult{
			group:     grouping,
			reduction: []reflect.Value{r.value},
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// findOneQuery finds the first row matching the query and writes it into `result` (pointer).
func (s *Store) findOneQuery(snap *pebble.Snapshot, result interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
	}
	originalLimit := query.limit
	query.limit = 1
	query.writable = false

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}

	elType := resultVal.Elem().Type()
	tp := elType
	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	keyField, hasKeyField := getKeyField(tp)
	val := reflect.New(tp)

	found := false
	if err := s.runQuery(snap, val.Interface(), query, nil, query.skip, func(r *record) error {
		found = true
		var rowValue reflect.Value
		if elType.Kind() == reflect.Ptr {
			rowValue = r.value
		} else {
			rowValue = r.value.Elem()
		}

		if hasKeyField {
			rowKey := rowValue
			for rowKey.Kind() == reflect.Ptr {
				rowKey = rowKey.Elem()
			}
			if err := s.decodeKey(r.key, rowKey.FieldByName(keyField.Name).Addr().Interface(), tp.Name()); err != nil {
				return err
			}
		}

		resultVal.Elem().Set(r.value.Elem())
		return nil
	}); err != nil {
		return err
	}
	query.limit = originalLimit

	if !found {
		return ErrNotFound
	}
	return nil
}

// forEach runs `fn` for each matched row in read-only mode.
func (s *Store) forEach(snap *pebble.Snapshot, query *Query, fn interface{}) error {
	if query == nil {
		query = &Query{}
	}

	fnVal := reflect.ValueOf(fn)
	argType := reflect.TypeOf(fn).In(0)
	if argType.Kind() == reflect.Ptr {
		argType = argType.Elem()
	}

	keyField, hasKeyField := getKeyField(argType)
	dataType := reflect.New(argType).Interface()
	storer := s.newStorer(dataType)

	return s.runQuery(snap, dataType, query, nil, query.skip, func(r *record) error {
		if hasKeyField {
			if err := s.decodeKey(r.key, r.value.Elem().FieldByName(keyField.Name).Addr().Interface(), storer.Type()); err != nil {
				return err
			}
		}
		out := fnVal.Call([]reflect.Value{r.value})
		if len(out) != 1 {
			return fmt.Errorf("foreach function does not return an error")
		}
		if out[0].IsNil() {
			return nil
		}
		return out[0].Interface().(error)
	})
}

// countQuery counts matched rows.
func (s *Store) countQuery(snap *pebble.Snapshot, dataType interface{}, query *Query) (uint64, error) {
	if query == nil {
		query = &Query{}
	}
	var count uint64
	if err := s.runQuery(snap, dataType, query, nil, query.skip, func(r *record) error {
		count++
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

// findByIndexQuery resolves index keys into primary keys, loads rows, and fills the result slice.
func (s *Store) findByIndexQuery(snap *pebble.Snapshot, resultSlice reflect.Value, query *Query) (err error) {
	criteria := query.fieldCriteria[query.index][0]
	sliceType := resultSlice.Elem().Type()
	query.dataType = dereference(sliceType.Elem())

	data := reflect.New(query.dataType).Interface()
	storer := s.newStorer(data)
	if err := query.validateIndex(data); err != nil {
		return err
	}
	if err := validateSortFields(query); err != nil {
		return err
	}

	var keyList KeyList
	if criteria.operator == in {
		keyList, err = s.fetchIndexValues(snap, query, storer.Type(), criteria.values...)
	} else {
		keyList, err = s.fetchIndexValues(snap, query, storer.Type(), criteria.value)
	}
	if err != nil {
		return err
	}

	keyField, hasKeyField := getKeyField(query.dataType)
	slice := reflect.MakeSlice(sliceType, 0, len(keyList))

	for i := range keyList {
		val, closer, err := snap.Get(keyList[i])
		if err == pebble.ErrNotFound {
			panic("inconsistency between keys stored in index and in Pebble directly")
		}
		if err != nil {
			return err
		}

		newElement := reflect.New(query.dataType)
		if err := s.decode(val, newElement.Interface()); err != nil {
			_ = closer.Close()
			return err
		}
		_ = closer.Close()

		if hasKeyField {
			if err := s.setKeyField(keyList[i], newElement, keyField, storer.Type()); err != nil {
				return err
			}
		}

		ok, err := query.matchesAllFields(s, keyList[i], newElement, newElement.Interface())
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if sliceType.Elem().Kind() != reflect.Ptr {
			newElement = newElement.Elem()
		}
		slice = reflect.Append(slice, newElement)
	}

	if len(query.sort) > 0 {
		sort.Slice(slice.Interface(), func(i, j int) bool {
			return sortFunction(query, slice.Index(i), slice.Index(j))
		})
	}

	startIndex, endIndex := getSkipAndLimitRange(query, slice.Len())
	slice = slice.Slice(startIndex, endIndex)

	resultSlice.Elem().Set(slice)
	return nil
}

// fetchIndexValues loads KeyList for given index keys from the index area.
func (s *Store) fetchIndexValues(snap *pebble.Snapshot, query *Query, typeName string, indexKeys ...interface{}) (KeyList, error) {
	keyList := KeyList{}
	for i := range indexKeys {
		indexKeyValue, err := s.encode(indexKeys[i])
		if err != nil {
			return nil, err
		}
		indexKey := newIndexKey(typeName, query.index, indexKeyValue)

		val, closer, err := snap.Get(indexKey)
		if err == pebble.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}

		indexValue := KeyList{}
		if err := s.decode(val, &indexValue); err != nil {
			_ = closer.Close()
			return nil, err
		}
		_ = closer.Close()

		keyList = append(keyList, indexValue...)
	}
	return keyList, nil
}

func (s *Store) setKeyField(data []byte, key reflect.Value, keyField reflect.StructField, typeName string) error {
	return s.decodeKey(data, key.Elem().FieldByName(keyField.Name).Addr().Interface(), typeName)
}

func dereference(value reflect.Type) reflect.Type {
	result := value
	for result.Kind() == reflect.Ptr {
		result = result.Elem()
	}
	return result
}
