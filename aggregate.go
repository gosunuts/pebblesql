package pebblesql

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/cockroachdb/pebble"
)

// AggregateResult holds a single aggregate group's values and grouping keys.
type AggregateResult struct {
	reduction []reflect.Value // always pointers
	group     []reflect.Value
	sortby    string
}

// Group copies the grouping key values into the provided pointers.
func (a *AggregateResult) Group(result ...interface{}) {
	for i := range result {
		resultVal := reflect.ValueOf(result[i])
		if resultVal.Kind() != reflect.Ptr {
			panic("result argument must be an address")
		}
		if i >= len(a.group) {
			panic(fmt.Sprintf("there is not %d elements in the grouping", i))
		}
		resultVal.Elem().Set(a.group[i])
	}
}

// Reduction appends the group's records into `result` (pointer to slice).
func (a *AggregateResult) Reduction(result interface{}) {
	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	sliceVal := resultVal.Elem()
	elType := sliceVal.Type().Elem()

	for i := range a.reduction {
		if elType.Kind() == reflect.Ptr {
			sliceVal = reflect.Append(sliceVal, a.reduction[i])
		} else {
			sliceVal = reflect.Append(sliceVal, a.reduction[i].Elem())
		}
	}
	resultVal.Elem().Set(sliceVal.Slice(0, sliceVal.Len()))
}

type aggregateResultSort AggregateResult

func (a *aggregateResultSort) Len() int { return len(a.reduction) }
func (a *aggregateResultSort) Swap(i, j int) {
	a.reduction[i], a.reduction[j] = a.reduction[j], a.reduction[i]
}
func (a *aggregateResultSort) Less(i, j int) bool {
	// reduction values are always pointers
	iVal := a.reduction[i].Elem().FieldByName(a.sortby)
	if !iVal.IsValid() {
		panic(fmt.Sprintf("the field %s does not exist in the type %s", a.sortby, a.reduction[i].Type()))
	}

	jVal := a.reduction[j].Elem().FieldByName(a.sortby)
	if !jVal.IsValid() {
		panic(fmt.Sprintf("the field %s does not exist in the type %s", a.sortby, a.reduction[j].Type()))
	}

	c, err := compare(iVal.Interface(), jVal.Interface())
	if err != nil {
		panic(err)
	}
	return c == -1
}

// Sort sorts the reduction by `field` ascending. Called by Min/Max automatically.
func (a *AggregateResult) Sort(field string) {
	if !startsUpper(field) {
		panic("the first letter of a field must be upper-case")
	}
	if a.sortby == field {
		return // already sorted
	}
	a.sortby = field
	sort.Sort((*aggregateResultSort)(a))
}

// Max sets `result` to the max record by `field`.
func (a *AggregateResult) Max(field string, result interface{}) {
	a.Sort(field)

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}
	if resultVal.IsNil() {
		panic("result argument must not be nil")
	}

	resultVal.Elem().Set(a.reduction[len(a.reduction)-1].Elem())
}

// Min sets `result` to the min record by `field`.
func (a *AggregateResult) Min(field string, result interface{}) {
	a.Sort(field)

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}
	if resultVal.IsNil() {
		panic("result argument must not be nil")
	}

	resultVal.Elem().Set(a.reduction[0].Elem())
}

// Avg returns the average of `field` over the group (as float64).
func (a *AggregateResult) Avg(field string) float64 {
	sum := a.Sum(field)
	return sum / float64(len(a.reduction))
}

// Sum returns the sum of `field` over the group (as float64).
func (a *AggregateResult) Sum(field string) float64 {
	var sum float64
	for i := range a.reduction {
		fVal := a.reduction[i].Elem().FieldByName(field)
		if !fVal.IsValid() {
			panic(fmt.Sprintf("the field %s does not exist in the type %s", field, a.reduction[i].Type()))
		}
		sum += tryFloat(fVal)
	}
	return sum
}

// Count returns the number of records in the group.
func (a *AggregateResult) Count() uint64 {
	return uint64(len(a.reduction))
}

// FindAggregate executes an aggregate query and returns results grouped by optional `groupBy` fields.
func (s *Store) FindAggregate(dataType interface{}, query *Query, groupBy ...string) ([]*AggregateResult, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	return s.TxFindAggregate(snap, dataType, query, groupBy...)
}

// TxFindAggregate is the same as FindAggregate but uses the provided snapshot.
func (s *Store) TxFindAggregate(snap *pebble.Snapshot, dataType interface{}, query *Query,
	groupBy ...string) ([]*AggregateResult, error) {
	return s.aggregateQuery(snap, dataType, query, groupBy...)
}

func tryFloat(val reflect.Value) float64 {
	switch val.Kind() {
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		return float64(val.Int())
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
		return float64(val.Uint())
	case reflect.Float32, reflect.Float64:
		return val.Float()
	default:
		panic(fmt.Sprintf("the field is of kind %s and cannot be converted to a float64", val.Kind()))
	}
}
