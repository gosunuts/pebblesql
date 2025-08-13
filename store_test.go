package pebblesql

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/* ---------------------------- helpers & harness ---------------------------- */

// testWrap opens a fresh Pebble-backed store per test.
func testWrap(t *testing.T, fn func(store *Store, t *testing.T)) {
	t.Helper()

	dir := t.TempDir()
	opts := &pebble.Options{}
	store, err := Open(Options{
		DirName:       dir,
		PebbleOptions: opts,
		Encoder:       DefaultEncode,
		Decoder:       DefaultDecode,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	fn(store, t)
}

/* -------------------------------- test model ------------------------------ */

type ItemTest struct {
	Key         int
	ID          int
	Name        string
	Category    string `badgerholdIndex:"Category"` // kept tag for compatibility with reflection
	Created     time.Time
	Tags        []string
	Color       string
	Fruit       string
	UpdateField string
	UpdateIndex string `badgerholdIndex:"UpdateIndex"`
	MapVal      map[string]string
}

func (i *ItemTest) equal(other *ItemTest) bool {
	if i.ID != other.ID {
		return false
	}
	if i.Name != other.Name {
		return false
	}
	if i.Category != other.Category {
		return false
	}
	if !i.Created.Equal(other.Created) {
		return false
	}
	return true
}

var testData = []ItemTest{
	{Key: 0, ID: 0, Name: "car", Category: "vehicle", Created: time.Now().AddDate(-1, 0, 0)},
	{Key: 1, ID: 1, Name: "truck", Category: "vehicle", Created: time.Now().AddDate(0, 30, 0)},
	{Key: 2, Name: "seal", Category: "animal", Created: time.Now().AddDate(-1, 0, 0)},
	{Key: 3, ID: 3, Name: "van", Category: "vehicle", Created: time.Now().AddDate(0, 30, 0)},
	{Key: 4, ID: 8, Name: "pizza", Category: "food", Created: time.Now(), Tags: []string{"cooked", "takeout"}},
	{Key: 5, ID: 1, Name: "crow", Category: "animal", Created: time.Now(), Color: "blue", Fruit: "orange"},
	{Key: 6, ID: 5, Name: "van", Category: "vehicle", Created: time.Now(), Color: "orange", Fruit: "orange"},
	{Key: 7, ID: 5, Name: "pizza", Category: "food", Created: time.Now(), Tags: []string{"cooked", "takeout"}},
	{Key: 8, ID: 6, Name: "lion", Category: "animal", Created: time.Now().AddDate(3, 0, 0)},
	{Key: 9, ID: 7, Name: "bear", Category: "animal", Created: time.Now().AddDate(3, 0, 0)},
	{Key: 10, ID: 9, Name: "tacos", Category: "food", Created: time.Now().AddDate(-3, 0, 0), Tags: []string{"cooked", "takeout"}, Color: "orange"},
	{Key: 11, ID: 10, Name: "golf cart", Category: "vehicle", Created: time.Now().AddDate(0, 0, 30), Color: "pink", Fruit: "apple"},
	{Key: 12, ID: 11, Name: "oatmeal", Category: "food", Created: time.Now().AddDate(0, 0, -30), Tags: []string{"cooked", "healthy"}},
	{Key: 13, ID: 8, Name: "mouse", Category: "animal", Created: time.Now()},
	{Key: 14, ID: 12, Name: "fish", Category: "animal", Created: time.Now().AddDate(0, 0, -1)},
	{Key: 15, ID: 13, Name: "fish", Category: "food", Created: time.Now(), Tags: []string{"cooked", "healthy"}},
	{Key: 16, ID: 9, Name: "zebra", Category: "animal", Created: time.Now(), MapVal: map[string]string{"test": "testval"}},
}

func insertTestData(t *testing.T, store *Store) {
	for i := range testData {
		err := store.Insert(testData[i].Key, testData[i])
		require.NoError(t, err)
	}
}

/* ------------------------------ query tests ------------------------------- */

type queryTest struct {
	name   string
	query  *Query
	result []int // indices into testData expected in results
}

func buildQueryTests() []queryTest {
	return []queryTest{
		{name: "Equal Key", query: Where(Key).Eq(testData[4].Key), result: []int{4}},
		{name: "Equal Field Without Index", query: Where("Name").Eq(testData[1].Name), result: []int{1}},
		{name: "Equal Field With Index", query: Where("Category").Eq("vehicle"), result: []int{0, 1, 3, 6, 11}},
		{name: "Not Equal Key", query: Where(Key).Ne(testData[4].Key), result: []int{0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{name: "Not Equal Field Without Index", query: Where("Name").Ne(testData[1].Name), result: []int{0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{name: "Not Equal Field With Index", query: Where("Category").Ne("vehicle"), result: []int{2, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16}},
		{name: "Greater Than Key", query: Where(Key).Gt(testData[10].Key), result: []int{11, 12, 13, 14, 15, 16}},
		{name: "Greater Than Field Without Index", query: Where("ID").Gt(10), result: []int{12, 14, 15}},
		{name: "Greater Than Field With Index", query: Where("Category").Gt("food"), result: []int{0, 1, 3, 6, 11}},
		{name: "Less Than Key", query: Where(Key).Lt(testData[0].Key), result: []int{}},
		{name: "Less Than Field Without Index", query: Where("ID").Lt(5), result: []int{0, 1, 2, 3, 5}},
		{name: "Less Than Field With Index", query: Where("Category").Lt("food"), result: []int{2, 5, 8, 9, 13, 14, 16}},
		{name: "Less Or Equal Key", query: Where(Key).Le(testData[0].Key), result: []int{0}},
		{name: "Less Or Equal Field", query: Where("ID").Le(5), result: []int{0, 1, 2, 3, 5, 6, 7}},
		{name: "Less Or Equal Field With Index", query: Where("Category").Le("food"), result: []int{2, 5, 8, 9, 13, 14, 16, 4, 7, 10, 12, 15}},
		{name: "Greater Or Equal Key", query: Where(Key).Ge(testData[10].Key), result: []int{10, 11, 12, 13, 14, 15, 16}},
		{name: "Greater Or Equal Field", query: Where("ID").Ge(10), result: []int{11, 12, 14, 15}},
		{name: "Greater Or Equal Field With Index", query: Where("Category").Ge("food"), result: []int{0, 1, 3, 6, 11, 4, 7, 10, 12, 15}},
		{name: "In", query: Where("ID").In(5, 8, 3), result: []int{3, 6, 7, 4, 13}},
		{name: "In cross-index", query: Where("ID").In(5, 8, 3).Index("Category"), result: []int{3, 6, 7, 4, 13}},
		{name: "In on index", query: Where("Category").In("food", "animal").Index("Category"), result: []int{4, 2, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16}},
		{name: "RegExp", query: Where("Name").RegExp(regexp.MustCompile("ea")), result: []int{2, 9, 12}},
		{
			name: "MatchFunc Field",
			query: Where("Name").MatchFunc(func(ra *RecordAccess) (bool, error) {
				field := ra.Field()
				s, ok := field.(string)
				if !ok {
					return false, fmt.Errorf("field not string: %T", field)
				}
				return strings.HasPrefix(s, "oat"), nil
			}),
			result: []int{12},
		},
		{
			name: "MatchFunc Record",
			query: Where("ID").MatchFunc(func(ra *RecordAccess) (bool, error) {
				rec, ok := ra.Record().(*ItemTest)
				if !ok {
					return false, fmt.Errorf("record not ItemTest: %T", ra.Record())
				}
				return strings.HasPrefix(rec.Name, "oat"), nil
			}),
			result: []int{12},
		},
		{
			name: "MatchFunc SubQuery",
			query: Where("Name").MatchFunc(func(ra *RecordAccess) (bool, error) {
				rec, ok := ra.Record().(*ItemTest)
				if !ok {
					return false, fmt.Errorf("record not ItemTest: %T", ra.Record())
				}
				var sub []ItemTest
				if err := ra.SubQuery(&sub, Where("Name").Eq(rec.Name).And("Category").Ne(rec.Category)); err != nil {
					return false, err
				}
				return len(sub) > 0, nil
			}),
			result: []int{14, 15},
		},
		{name: "Time Comparison", query: Where("Created").Gt(time.Now()), result: []int{1, 3, 8, 9, 11}},
		{name: "Chained And (non-index lead)", query: Where("Created").Gt(time.Now()).And("Category").Eq("vehicle"), result: []int{1, 3, 11}},
		{name: "Chained And (three)", query: Where("Created").Gt(time.Now()).And("Category").Eq("vehicle").And("ID").Ge(10), result: []int{11}},
		{name: "Chained And with index lead", query: Where("Category").Eq("vehicle").And("ID").Ge(10).And("Created").Gt(time.Now()), result: []int{11}},
		{name: "Chained Or with index", query: Where("Category").Eq("vehicle").Or(Where("Category").Eq("animal")), result: []int{0, 1, 3, 6, 11, 2, 5, 8, 9, 13, 14, 16}},
		{name: "Chained Or with union", query: Where("Category").Eq("animal").Or(Where("Name").Eq("fish")), result: []int{2, 5, 8, 9, 13, 14, 16, 15}},
		{
			name: "Mixed And+Or",
			query: Where("Category").Eq("animal").And("Created").Gt(time.Now()).
				Or(Where("Name").Eq("fish").And("ID").Ge(13)),
			result: []int{8, 9, 15},
		},
		{name: "Nil Query", query: nil, result: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{name: "IsNil", query: Where("Tags").IsNil(), result: []int{0, 1, 2, 3, 5, 6, 8, 9, 11, 13, 14, 16}},
		{name: "HasPrefix", query: Where("Name").HasPrefix("golf"), result: []int{11}},
		{name: "HasSuffix", query: Where("Name").HasSuffix("cart"), result: []int{11}},
		{name: "Self-field compare", query: Where("Color").Eq(Field("Fruit")).And("Fruit").Ne(""), result: []int{6}},
		{name: "Index+Key secondary test", query: Where("Category").Eq("food").And(Key).Eq(testData[4].Key), result: []int{4}},
		{name: "Skip", query: Where(Key).Gt(testData[10].Key).Skip(3), result: []int{14, 15, 16}},
		{name: "Skip Past Len", query: Where(Key).Gt(testData[10].Key).Skip(9), result: []int{}},
		{name: "Skip with Or", query: Where("Category").Eq("vehicle").Or(Where("Category").Eq("animal")).Skip(4), result: []int{11, 2, 5, 8, 9, 13, 14, 16}},
		{name: "Skip across Or", query: Where("Category").Eq("vehicle").Or(Where("Category").Eq("animal")).Skip(8), result: []int{16, 9, 13, 14}},
		{name: "Limit", query: Where(Key).Gt(testData[10].Key).Limit(5), result: []int{11, 12, 13, 14, 15}},
		{
			name: "Issue #8 - MatchFunc on index",
			query: Where("Category").MatchFunc(func(ra *RecordAccess) (bool, error) {
				field := ra.Field()
				s, ok := field.(string)
				if !ok {
					return false, fmt.Errorf("field not string: %T", field)
				}
				return !strings.HasPrefix(s, "veh"), nil
			}),
			result: []int{2, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16},
		},
		{
			name: "Issue #8 - MatchFunc on specific index",
			query: Where("Category").MatchFunc(func(ra *RecordAccess) (bool, error) {
				field := ra.Field()
				s, ok := field.(string)
				if !ok {
					return false, fmt.Errorf("field not string: %T", field)
				}
				return !strings.HasPrefix(s, "veh"), nil
			}).Index("Category"),
			result: []int{2, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16},
		},
		{name: "Indexed In", query: Where("Category").In("animal", "vehicle"), result: []int{0, 1, 2, 3, 5, 6, 8, 9, 11, 13, 14, 16}},
		{name: "Equal With Specific Index", query: Where("Category").Eq("vehicle").Index("Category"), result: []int{0, 1, 3, 6, 11}},
		{name: "Key after field", query: Where("Category").Eq("food").And(Key).Gt(testData[10].Key), result: []int{12, 15}},
		{name: "Key after index", query: Where("Category").Eq("food").Index("Category").And(Key).Gt(testData[10].Key), result: []int{12, 15}},
		{name: "Contains", query: Where("Tags").Contains("takeout"), result: []int{4, 7, 10}},
		{name: "Contains Any", query: Where("Tags").ContainsAny("takeout", "healthy"), result: []int{4, 7, 10, 12, 15}},
		{name: "Contains All (none)", query: Where("Tags").ContainsAll("takeout", "healthy"), result: []int{}},
		{name: "Contains All (two)", query: Where("Tags").ContainsAll("cooked", "healthy"), result: []int{12, 15}},
		{name: "Slice helper", query: Where("Tags").ContainsAll(Slice([]string{"cooked", "healthy"})...), result: []int{12, 15}},
		{name: "Contains on non-slice", query: Where("Category").Contains("cooked"), result: []int{}},
		{name: "Map Has Key", query: Where("MapVal").HasKey("test"), result: []int{16}},
		{name: "Map Has Key miss", query: Where("MapVal").HasKey("other"), result: []int{}},
		{name: "Keys with In", query: Where(Key).In(1, 2, 3), result: []int{1, 2, 3}},
	}
}

func TestFind(t *testing.T) {
	for _, tst := range buildQueryTests() {
		testWrap(t, func(store *Store, t *testing.T) {
			insertTestData(t, store)

			var result []ItemTest
			err := store.Find(&result, tst.query)
			require.NoError(t, err)
			require.Equal(t, len(tst.result), len(result))
			for _, r := range result {
				found := false
				for _, idx := range tst.result {
					if r.equal(&testData[idx]) {
						found = true
						break
					}
				}
				assert.True(t, found, "%v should not be in result set", r)
			}
		})
	}
}

/* --------------------------- misc query guardrails ------------------------- */

type BadType struct{}

func TestFindOnUnknownType(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)
		var result []BadType
		require.NoError(t, store.Find(&result, Where("BadName").Eq("blah")))
		require.Equal(t, 0, len(result))
	})
}

func TestFindWithNilValueTypeMismatch(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)
		var result []ItemTest
		err := store.Find(&result, Where("Name").Eq(nil))
		require.Error(t, err)
	})
}

func TestFindWithNonSlicePtrPanics(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		require.Panics(t, func() {
			var result []ItemTest
			_ = store.Find(result, Where("Name").Eq("blah"))
		})
	})
}

func TestQueryWhereLowerPanic(t *testing.T) {
	require.Panics(t, func() {
		_ = Where("lower").Eq("test")
	})
}

func TestQueryAndLowerPanic(t *testing.T) {
	require.Panics(t, func() {
		_ = Where("Upper").Eq("test").And("lower").Eq("test")
	})
}

func TestFindInvalidFieldName(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)
		var result []ItemTest
		err := store.Find(&result, Where("BadFieldName").Eq("test"))
		require.Error(t, err)
	})
}

func TestFindInvalidIndex(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)
		var result []ItemTest
		err := store.Find(&result, Where("Name").Eq("test").Index("BadIndex"))
		require.Error(t, err)
	})
}

func TestFindEmptyBucketWithIndex(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		var result []ItemTest
		require.NoError(t, store.Find(&result, Where("Category").Eq("animal").Index("Category")))
		require.Equal(t, 0, len(result))
	})
}

func TestQueryStringPrint(t *testing.T) {
	q := Where("FirstField").Eq("first value").And("SecondField").Gt("Second Value").And("ThirdField").
		Lt("Third Value").And("FourthField").Ge("FourthValue").And("FifthField").Le("FifthValue").And("SixthField").
		Ne("Sixth Value").Or(Where("FirstField").In("val1", "val2", "val3").And("SecondField").IsNil().
		And("ThirdField").RegExp(regexp.MustCompile("test")).Index("IndexName").And("FirstField").
		MatchFunc(func(ra *RecordAccess) (bool, error) { return true, nil })).
		And("SeventhField").HasPrefix("SeventhValue").And("EighthField").HasSuffix("EighthValue")

	contains := []string{
		"FirstField == first value",
		"SecondField > Second Value",
		"ThirdField < Third Value",
		"FourthField >= FourthValue",
		"FifthField <= FifthValue",
		"SixthField != Sixth Value",
		"FirstField in [val1 val2 val3]",
		"FirstField matches the function",
		"SecondField is nil",
		"ThirdField matches the regular expression",
		"Using Index [IndexName]",
		"SeventhField starts with SeventhValue",
		"EighthField ends with EighthValue",
	}
	got := q.String()
	lines := strings.Split(got, "\n")
	for _, needle := range contains {
		found := false
		for _, ln := range lines {
			if strings.Contains(ln, needle) {
				found = true
				break
			}
		}
		assert.Truef(t, found, "line %q not found in:\n%s", needle, got)
	}
}

/* -------------------------- skip/limit and guards ------------------------- */

func TestSkip(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)
		baseQ := Where("Category").Eq("animal").Or(Where("Name").Eq("fish"))
		var full []ItemTest
		require.NoError(t, store.Find(&full, baseQ))

		var skipped []ItemTest
		skip := 5
		require.NoError(t, store.Find(&skipped, baseQ.Skip(skip)))
		require.Equal(t, len(full)-skip, len(skipped))

		full = full[skip:]
		for i := range skipped {
			found := false
			for k := range full {
				if skipped[i].equal(&full[k]) {
					found = true
					break
				}
			}
			assert.True(t, found, "%v not found in baseline after skip", skipped[i])
		}
	})
}

func TestSkipNegativePanics(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		require.Panics(t, func() {
			var out []ItemTest
			_ = store.Find(&out, Where("Name").Eq("blah").Skip(-1))
		})
	})
}

func TestLimitNegativePanics(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		require.Panics(t, func() {
			var out []ItemTest
			_ = store.Find(&out, Where("Name").Eq("blah").Limit(-1))
		})
	})
}

func TestSkipDoublePanics(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		require.Panics(t, func() {
			var out []ItemTest
			_ = store.Find(&out, Where("Name").Eq("blah").Skip(1).Skip(2))
		})
	})
}

func TestLimitDoublePanics(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		require.Panics(t, func() {
			var out []ItemTest
			_ = store.Find(&out, Where("Name").Eq("blah").Limit(1).Limit(2))
		})
	})
}

func TestSkipInOrPanics(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		require.Panics(t, func() {
			var out []ItemTest
			_ = store.Find(&out, Where("Name").Eq("blah").Or(Where("Name").Eq("blah").Skip(3)))
		})
	})
}

func TestLimitInOrPanics(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		require.Panics(t, func() {
			var out []ItemTest
			_ = store.Find(&out, Where("Name").Eq("blah").Or(Where("Name").Eq("blah").Limit(3)))
		})
	})
}

/* ------------------------------- slice/ptr out ---------------------------- */

func TestSlicePointerResult(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		count := 10
		for i := 0; i < count; i++ {
			require.NoError(t, store.Insert(i, &ItemTest{Key: i, ID: i}))
		}
		var result []*ItemTest
		require.NoError(t, store.Find(&result, nil))
		require.Equal(t, count, len(result))
	})
}

/* ----------------------------- key matchfunc guard ------------------------ */

func TestKeyMatchFuncPanics(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		require.Panics(t, func() {
			var result []ItemTest
			_ = store.Find(&result, Where(Key).MatchFunc(func(ra *RecordAccess) (bool, error) {
				_, ok := ra.Field().(string)
				if !ok {
					return false, fmt.Errorf("not string")
				}
				return true, nil
			}))
		})
	})
}

/* ----------------------------- key tag population ------------------------- */

func TestKeyStructTag_SetOnFind(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		type KeyTest struct {
			Key   int `badgerholdKey:"Key"`
			Value string
		}
		key := 3
		require.NoError(t, store.Insert(key, &KeyTest{Value: "test value"}))
		var result []KeyTest
		require.NoError(t, store.Find(&result, Where(Key).Eq(key)))
		require.Equal(t, 1, len(result))
		require.Equal(t, key, result[0].Key)
	})
}

func TestKeyStructTagPtr_SetOnFind(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		type KeyTest struct {
			Key   *int `badgerholdKey:"Key"`
			Value string
		}
		key := 3
		require.NoError(t, store.Insert(&key, &KeyTest{Value: "test value"}))
		var result []KeyTest
		require.NoError(t, store.Find(&result, Where(Key).Eq(key)))
		require.Equal(t, 1, len(result))
		require.NotNil(t, result[0].Key)
		require.Equal(t, key, *result[0].Key)
	})
}

func TestGetKeyStructTagVariants(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		type KeyTest struct {
			Key   int `badgerholdKey:"Key"`
			Value string
		}
		key := 3
		require.NoError(t, store.Insert(key, &KeyTest{Value: "test value"}))
		var out KeyTest
		require.NoError(t, store.Get(key, &out))
		require.Equal(t, key, out.Key)
	})
}

func TestGetKeyStructTagPtr(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		type KeyTest struct {
			Key   *int `badgerholdKey:"Key"`
			Value string
		}
		key := 5
		require.NoError(t, store.Insert(&key, &KeyTest{Value: "test value"}))
		var out KeyTest
		require.NoError(t, store.Get(key, &out))
		require.NotNil(t, out.Key)
		require.Equal(t, key, *out.Key)
	})
}

/* ---------------------------- FindOne / Count ----------------------------- */

func TestFindOne(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)
		for _, tst := range buildQueryTests() {
			res := &ItemTest{}
			err := store.FindOne(res, tst.query)
			if len(tst.result) == 0 {
				require.ErrorIs(t, err, ErrNotFound)
				return
			}
			require.NoError(t, err)
			assert.True(t, res.equal(&testData[tst.result[0]]), "FindOne mismatch: got=%+v want=%+v", res, testData[tst.result[0]])
		}
	})
}

func TestFindOneNonPtrPanics(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		require.Panics(t, func() {
			var res ItemTest
			_ = store.FindOne(res, Where("Name").Eq("x"))
		})
	})
}

func TestCount(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)
		for _, tst := range buildQueryTests() {
			t.Run(tst.name, func(t *testing.T) {
				count, err := store.Count(ItemTest{}, tst.query)
				require.NoError(t, err)
				require.Equal(t, uint64(len(tst.result)), count)
			})
		}
	})
}

/* ------------------------------- issue checks ----------------------------- */

func TestIssue74_HasPrefixSuffixOnKeys(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		type Item struct {
			ID       string
			Category string `badgerholdIndex:"Category"`
			Created  time.Time
		}
		id := "test_1_test"
		require.NoError(t, store.Insert(id, &Item{ID: id}))
		tmp := &Item{}
		require.NoError(t, store.FindOne(tmp, Where(Key).HasPrefix("test")))
		require.NoError(t, store.FindOne(tmp, Where(Key).HasSuffix("test")))
	})
}

func TestFindIndexedWithSortSkipLimit(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)
		results := make([]ItemTest, 0, 3)
		require.NoError(t, store.Find(
			&results,
			Where("Category").Eq("vehicle").Index("Category").SortBy("Name").Skip(1).Limit(3),
		))
		expectedIdx := []int{11, 1, 3}
		require.Equal(t, len(expectedIdx), len(results))
		for i := range results {
			assert.True(t, testData[expectedIdx[i]].equal(&results[i]), "incorrect row at %d", i)
		}
	})
}

func TestFindIndexedResultSliceOfPointers(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)
		results := make([]*ItemTest, 0, 3)
		require.NoError(t, store.Find(&results, Where("Category").Eq("vehicle").Index("Category")))
		expected := []int{0, 1, 3, 6, 11}
		require.Equal(t, len(expected), len(results))
		for i := range results {
			assert.True(t, testData[expected[i]].equal(results[i]), "incorrect row at %d", i)
		}
	})
}

/* ------------------------------ storer impl ------------------------------- */

type ItemWithStorer struct {
	Name string
}

func (i ItemWithStorer) Type() string { return "ItemWithStorer" }
func (i ItemWithStorer) Indexes() map[string]Index {
	return map[string]Index{
		"Name": {
			IndexFunc: func(name string, value interface{}) ([]byte, error) {
				v := value.(*ItemWithStorer)
				return DefaultEncode(v.Name)
			},
			Unique: false,
		},
	}
}

func TestFindWithStorerImplementation(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		custom := &ItemWithStorer{Name: "pizza"}
		require.NoError(t, store.Insert(1, custom))
		results := make([]ItemWithStorer, 1)
		require.NoError(t, store.Find(&results, Where("Name").Eq("pizza").Index("Name")))
		require.Equal(t, *custom, results[0])
	})
}

/* ----------------------------- query.Matches() ---------------------------- */

type queryMatchTest struct {
	Key     int `badgerholdKey:"Key"`
	Age     int
	Color   string
	Created time.Time
}

func TestComplexQueryMatch(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		item := queryMatchTest{Key: 1, Age: 2, Color: "color", Created: time.UnixMicro(0)}
		q := Where("Key").Eq(1).And("Age").Eq(3).Or(Where("Key").Eq(2).And("Age").Eq(2))
		got, err := q.Matches(store, item)
		require.NoError(t, err)
		require.False(t, got)

		q = Where("Key").Eq(1).And("Age").Eq(3).Or(Where("Key").Eq(1).And("Age").Eq(2))
		got, err = q.Matches(store, item)
		require.NoError(t, err)
		require.True(t, got)

		q = Where("Key").Eq(1).And("Age").Eq(1).
			Or(Where("Key").Eq(2).And("Age").Eq(2).
				Or(Where("Key").Eq(1).And("Age").Eq(2)))
		got, err = q.Matches(store, item)
		require.NoError(t, err)
		require.True(t, got)
	})
}

func TestQueryMatchVariants(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		item := queryMatchTest{Key: 1, Age: 2, Color: "color", Created: time.UnixMicro(0)}
		cases := []struct {
			q     *Query
			want  bool
			title string
		}{
			{Where("Key").Eq(1), true, "SingleKeyFieldMatch"},
			{Where("Key").Eq(2), false, "SingleKeyFieldMismatch"},
			{Where("Age").Eq(2), true, "SingleIntFieldMatch"},
			{Where("Age").Eq(3), false, "SingleIntFieldMismatch"},
			{Where("Key").Eq(1).And("Color").Eq("color"), true, "MultiFieldAndMatch"},
			{Where("Key").Eq(1).And("Color").Eq("notcolor"), false, "MultiFieldAndMismatch"},
			{Where("Key").Eq(2).Or(Where("Color").Eq("color")), true, "MultiFieldOrMatch"},
			{Where("Key").Eq(2).Or(Where("Color").Eq("notcolor")), false, "MultiFieldOrMismatch"},
			{Where("Created").Eq(time.UnixMicro(0)), true, "SingleTimeFieldMatch"},
			{Where("Created").Eq(time.UnixMicro(1)), false, "SingleTimeFieldMismatch"},
		}
		for _, tc := range cases {
			t.Run(tc.title+"Struct", func(t *testing.T) {
				got, err := tc.q.Matches(store, item)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
			t.Run(tc.title+"Ptr", func(t *testing.T) {
				got, err := tc.q.Matches(store, &item)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	})
}

/* ----------------------------- iterator stress ---------------------------- */

func TestQueryIterKeyCacheOverflow(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		type KeyCacheTest struct {
			Key      int
			IndexKey int `badgerholdIndex:"IndexKey"`
		}
		size := 200
		stop := 10
		for i := 0; i < size; i++ {
			require.NoError(t, store.Insert(i, &KeyCacheTest{Key: i, IndexKey: i}))
		}

		tests := []*Query{
			Where(Key).Gt(stop),
			Where(Key).Gt(stop).Index(Key),
			Where("Key").Gt(stop),
			Where("IndexKey").Gt(stop).Index("IndexKey"),
			Where("IndexKey").MatchFunc(func(ra *RecordAccess) (bool, error) {
				v, ok := ra.Field().(int)
				if !ok {
					return false, fmt.Errorf("field not int: %T", ra.Field())
				}
				return v > stop, nil
			}).Index("IndexKey"),
		}

		for ti, q := range tests {
			t.Run(fmt.Sprintf("iter-stress-%d", ti), func(t *testing.T) {
				var result []KeyCacheTest
				require.NoError(t, store.Find(&result, q))
				for i := 0; i < len(result); i++ {
					assert.Greater(t, result[i].Key, stop, "expected key > %d", stop)
				}
			})
		}
	})
}

/* ------------------------------- nested ptrs ------------------------------ */

func TestNestedStructPointer(t *testing.T) {
	type notification struct{ Enabled bool }
	type device struct {
		ID            string `badgerhold:"key"`
		Notifications *notification
	}
	testWrap(t, func(store *Store, t *testing.T) {
		id := "1"
		require.NoError(t, store.Insert(id, &device{ID: id, Notifications: &notification{Enabled: true}}))

		var devices []*device
		require.NoError(t, store.Find(&devices, nil))

		d := &device{}
		require.NoError(t, store.Get(id, d))

		require.Equal(t, id, devices[0].ID)
		require.NotNil(t, devices[0].Notifications)
		assert.True(t, devices[0].Notifications.Enabled)

		require.Equal(t, id, d.ID)
		require.NotNil(t, d.Notifications)
		assert.True(t, d.Notifications.Enabled)
	})
}

/* ------------------------- CRUD: Insert/Update/etc ------------------------ */

func TestCRUD_Insert_Get_Update_Upsert_Delete(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		key := 42
		row := &ItemTest{Key: key, ID: 100, Name: "alpha", Category: "groupA"}
		require.NoError(t, store.Insert(key, row))

		// Get
		var got ItemTest
		require.NoError(t, store.Get(key, &got))
		assert.True(t, got.equal(row), "get mismatch")

		// Update
		row2 := &ItemTest{Key: key, ID: 101, Name: "beta", Category: "groupB"}
		require.NoError(t, store.Update(key, row2))
		require.NoError(t, store.Get(key, &got))
		assert.True(t, got.equal(row2), "update mismatch")

		// Upsert (update path)
		row2.Name = "gamma"
		require.NoError(t, store.Upsert(key, row2))
		require.NoError(t, store.Get(key, &got))
		require.Equal(t, "gamma", got.Name)

		// Upsert (insert path)
		key2 := 77
		up := &ItemTest{Key: key2, ID: 501, Name: "delta", Category: "groupC"}
		require.NoError(t, store.Upsert(key2, up))
		require.NoError(t, store.Get(key2, &got))
		assert.True(t, got.equal(up), "upsert insert mismatch")

		// Delete
		require.NoError(t, store.Delete(key2, ItemTest{}))
		err := store.Get(key2, &got)
		require.Error(t, err, "expected not found after delete")
	})
}

func TestUpdateMatching_DeleteMatching(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)

		// UpdateMatching: set UpdateField="YES" on all vehicles
		require.NoError(t, store.UpdateMatching(ItemTest{}, Where("Category").Eq("vehicle"), func(rec interface{}) error {
			ptr := rec.(*ItemTest)
			ptr.UpdateField = "YES"
			return nil
		}))

		var vehicles []ItemTest
		require.NoError(t, store.Find(&vehicles, Where("Category").Eq("vehicle")))
		for _, v := range vehicles {
			require.Equal(t, "YES", v.UpdateField)
		}

		// DeleteMatching: remove all animals
		require.NoError(t, store.DeleteMatching(ItemTest{}, Where("Category").Eq("animal")))
		cnt, err := store.Count(ItemTest{}, Where("Category").Eq("animal"))
		require.NoError(t, err)
		require.Equal(t, uint64(0), cnt)
	})
}

/* -------------------------------- aggregate ------------------------------- */

func TestFindAggregate(t *testing.T) {
	testWrap(t, func(store *Store, t *testing.T) {
		insertTestData(t, store)

		// Group by Category, compute max ID within each group using Aggregate API.
		grps, err := store.FindAggregate(ItemTest{}, nil, "Category")
		require.NoError(t, err)
		require.Greater(t, len(grps), 0, "expected groups")

		// Build map Category -> max ID via helper
		maxByCat := map[string]int{}
		for _, g := range grps {
			var max ItemTest
			g.Max("ID", &max)
			var cat string
			g.Group(&cat) // first grouping field
			maxByCat[cat] = max.ID
		}

		// Expected from testData
		require.Equal(t, 10, maxByCat["vehicle"])
		require.Equal(t, 13, maxByCat["food"])
		require.Equal(t, 12, maxByCat["animal"])

		// Also verify Sum/Avg/Count/Reduction
		for _, g := range grps {
			_ = g.Sum("ID")
			_ = g.Avg("ID")
			_ = g.Count()
			var rows []ItemTest
			g.Reduction(&rows)
			assert.Greater(t, len(rows), 0, "reduction should have rows")
		}
	})
}
