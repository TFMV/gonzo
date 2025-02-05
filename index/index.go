package index

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	roaring "github.com/RoaringBitmap/roaring"
	bloom "github.com/bits-and-blooms/bloom/v3"
	murmur3 "github.com/spaolacci/murmur3"
)

// ---------------------------------------------------------------------
// Strategy: Defines which indexing strategy to use
// ---------------------------------------------------------------------

type Strategy int

const (
	RoaringBitmap Strategy = iota
	HashIndex
	Bloom
	SortedColumn
)

// ---------------------------------------------------------------------
// Index: The universal interface for all index implementations
// ---------------------------------------------------------------------

type Index interface {
	// Add inserts rowID for the given value into the index
	Add(rowID uint32, value interface{}) error
	// Remove removes rowID (and its associated value) from the index
	Remove(rowID uint32) error
	// Search returns all rowIDs matching the given value
	Search(value interface{}) ([]uint32, error)
	// Clear removes all entries
	Clear() error
}

// ---------------------------------------------------------------------
// IndexManager: Manages multiple indexes per column
// ---------------------------------------------------------------------

type IndexManager struct {
	mu       sync.RWMutex
	indexes  map[string]map[Strategy]Index
	settings IndexSettings
}

type IndexSettings struct {
	// BloomFilterFPRate is the desired false-positive rate for the Bloom filter
	BloomFilterFPRate float64
	// HashIndexSize is an (optional) hint for sizing a HashIndex
	HashIndexSize int
	// SortedBatchSize is an (optional) parameter for the SortedIndex
	SortedBatchSize int
}

// NewIndexManager creates a new index manager with the given settings
func NewIndexManager(settings IndexSettings) *IndexManager {
	return &IndexManager{
		indexes:  make(map[string]map[Strategy]Index),
		settings: settings,
	}
}

// CreateIndex instantiates a new index of the specified strategy for a given column
func (im *IndexManager) CreateIndex(column string, strategy Strategy) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if im.indexes[column] == nil {
		im.indexes[column] = make(map[Strategy]Index)
	}

	var idx Index
	switch strategy {
	case RoaringBitmap:
		idx = NewRoaringIndex()
	case HashIndex:
		idx = NewHashIndex(im.settings.HashIndexSize)
	case Bloom:
		idx = NewBloomIndex(im.settings.BloomFilterFPRate)
	case SortedColumn:
		idx = NewSortedIndex(im.settings.SortedBatchSize)
	default:
		return fmt.Errorf("unsupported index strategy: %v", strategy)
	}

	im.indexes[column][strategy] = idx
	return nil
}

// GetIndex retrieves an existing index for a given column and strategy
func (im *IndexManager) GetIndex(column string, strategy Strategy) (Index, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	strats, ok := im.indexes[column]
	if !ok {
		return nil, false
	}
	idx, exists := strats[strategy]
	return idx, exists
}

// ---------------------------------------------------------------------
// 1) Roaring Bitmap Index
//
//    Maps each distinct value -> roaring.Bitmap of rowIDs.
// ---------------------------------------------------------------------

type roaringIndex struct {
	mu     sync.RWMutex
	values map[interface{}]*roaring.Bitmap
	// For removal, track rowID -> value
	rowVal map[uint32]interface{}
}

// NewRoaringIndex constructs a new Index backed by multiple Roaring bitmaps
func NewRoaringIndex() Index {
	return &roaringIndex{
		values: make(map[interface{}]*roaring.Bitmap),
		rowVal: make(map[uint32]interface{}),
	}
}

func (r *roaringIndex) Add(rowID uint32, value interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	bm, ok := r.values[value]
	if !ok {
		bm = roaring.New()
		r.values[value] = bm
	}
	bm.Add(rowID)
	r.rowVal[rowID] = value
	return nil
}

func (r *roaringIndex) Remove(rowID uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	val, ok := r.rowVal[rowID]
	if !ok {
		// rowID not found
		return nil
	}
	bm := r.values[val]
	if bm != nil {
		bm.Remove(rowID)
		if bm.IsEmpty() {
			delete(r.values, val)
		}
	}
	delete(r.rowVal, rowID)
	return nil
}

func (r *roaringIndex) Search(value interface{}) ([]uint32, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	bm, ok := r.values[value]
	if !ok || bm == nil {
		return nil, nil
	}
	return bm.ToArray(), nil
}

func (r *roaringIndex) Clear() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.values = make(map[interface{}]*roaring.Bitmap)
	r.rowVal = make(map[uint32]interface{})
	return nil
}

// ---------------------------------------------------------------------
// 2) Hash Index
//
//    Uses Murmur3 to hash each value into a bucket, and within each bucket
//    stores value -> Roaring bitmap of rowIDs. Also tracks rowID -> (hash, value)
//    for removal.
// ---------------------------------------------------------------------

type hashIndex struct {
	mu       sync.RWMutex
	size     int
	buckets  map[uint64]map[interface{}]*roaring.Bitmap // hash -> map[value] -> bitmap
	rowHash  map[uint32]uint64                          // rowID -> hash
	rowValue map[uint32]interface{}                     // rowID -> original value
}

// NewHashIndex constructs a new HashIndex
func NewHashIndex(sizeHint int) Index {
	// sizeHint can be used to preallocate maps if desired.
	return &hashIndex{
		size:     sizeHint,
		buckets:  make(map[uint64]map[interface{}]*roaring.Bitmap, sizeHint),
		rowHash:  make(map[uint32]uint64, sizeHint),
		rowValue: make(map[uint32]interface{}, sizeHint),
	}
}

func (h *hashIndex) Add(rowID uint32, value interface{}) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := murmurKey(value)

	submap, ok := h.buckets[key]
	if !ok {
		submap = make(map[interface{}]*roaring.Bitmap)
		h.buckets[key] = submap
	}
	bm, ok := submap[value]
	if !ok {
		bm = roaring.New()
		submap[value] = bm
	}
	bm.Add(rowID)

	// Store rowID -> (hash, value) for removal
	h.rowHash[rowID] = key
	h.rowValue[rowID] = value

	return nil
}

func (h *hashIndex) Remove(rowID uint32) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	key, ok := h.rowHash[rowID]
	if !ok {
		// rowID not found
		return nil
	}
	value := h.rowValue[rowID]
	submap := h.buckets[key]
	if submap == nil {
		return nil
	}
	bm, ok := submap[value]
	if !ok {
		return nil
	}
	bm.Remove(rowID)
	if bm.IsEmpty() {
		delete(submap, value)
	}
	if len(submap) == 0 {
		delete(h.buckets, key)
	}

	delete(h.rowHash, rowID)
	delete(h.rowValue, rowID)
	return nil
}

func (h *hashIndex) Search(value interface{}) ([]uint32, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	key := murmurKey(value)
	submap, ok := h.buckets[key]
	if !ok {
		return nil, nil
	}
	bm, ok := submap[value]
	if !ok || bm == nil {
		return nil, nil
	}
	return bm.ToArray(), nil
}

func (h *hashIndex) Clear() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.buckets = make(map[uint64]map[interface{}]*roaring.Bitmap, h.size)
	h.rowHash = make(map[uint32]uint64, h.size)
	h.rowValue = make(map[uint32]interface{}, h.size)
	return nil
}

// murmurKey hashes a value to a 64-bit key.
// This simplistic approach converts the value to a string (if possible)
// before hashing. Production code might do specialized hashing.
func murmurKey(value interface{}) uint64 {
	var s string
	switch v := value.(type) {
	case string:
		s = v
	case fmt.Stringer:
		s = v.String()
	default:
		s = fmt.Sprintf("%v", v)
	}
	h64 := murmur3.Sum64([]byte(s))
	return h64
}

// ---------------------------------------------------------------------
// 3) Bloom Filter Index
//
//    A classic Bloom filter only tells us if an item is "possibly" in
//    the set or "definitely not." Here, we keep a map[value]->bitmap
//    for rowIDs, plus a bloom filter for quick membership checks.
//    Removal from a standard bloom.Filter is not possible, so `Remove`
//    only updates the map of rowIDs. This can lead to false positives
//    over time.
// ---------------------------------------------------------------------

type bloomIndex struct {
	mu         sync.RWMutex
	filter     *bloom.BloomFilter
	values     map[interface{}]*roaring.Bitmap
	rowValue   map[uint32]interface{}
	fpEstimate float64
}

func NewBloomIndex(fpRate float64) Index {
	// We'll guess an initial capacity of ~100_000 for demonstration.
	// In production, you'd pick a capacity or grow dynamically.
	const capacity = 100000
	filter := bloom.NewWithEstimates(uint(capacity), fpRate)
	return &bloomIndex{
		filter:     filter,
		values:     make(map[interface{}]*roaring.Bitmap),
		rowValue:   make(map[uint32]interface{}),
		fpEstimate: fpRate,
	}
}

func (b *bloomIndex) Add(rowID uint32, value interface{}) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Convert value to a string for bloom filter membership
	valStr := toString(value)
	b.filter.Add([]byte(valStr))

	bm, ok := b.values[value]
	if !ok {
		bm = roaring.New()
		b.values[value] = bm
	}
	bm.Add(rowID)
	b.rowValue[rowID] = value

	return nil
}

func (b *bloomIndex) Remove(rowID uint32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	val, ok := b.rowValue[rowID]
	if !ok {
		return nil
	}
	bm := b.values[val]
	if bm != nil {
		bm.Remove(rowID)
		if bm.IsEmpty() {
			delete(b.values, val)
		}
	}
	delete(b.rowValue, rowID)

	// Standard bloom filters don't support deletion; we do not remove from b.filter
	return nil
}

func (b *bloomIndex) Search(value interface{}) ([]uint32, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Quick membership test
	valStr := toString(value)
	if !b.filter.Test([]byte(valStr)) {
		// Definitely not present
		return nil, nil
	}

	// Possibly present, check the actual map
	bm, ok := b.values[value]
	if !ok || bm == nil {
		return nil, nil
	}
	return bm.ToArray(), nil
}

func (b *bloomIndex) Clear() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Recreate the bloom filter
	const capacity = 100000
	b.filter = bloom.NewWithEstimates(uint(capacity), b.fpEstimate)
	b.values = make(map[interface{}]*roaring.Bitmap)
	b.rowValue = make(map[uint32]interface{})
	return nil
}

func toString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ---------------------------------------------------------------------
// 4) Sorted Column Index
//
//    Stores (value, rowID) entries in a sorted slice by "value".
//    For demonstration, we support comparing int, float64, and string.
//    Insertions & deletions are O(n), searching is O(log n) by value.
// ---------------------------------------------------------------------

type sortedIndex struct {
	mu        sync.RWMutex
	entries   []sortedEntry
	batchSize int // not strictly used in this minimal example
}

type sortedEntry struct {
	value interface{}
	rowID uint32
}

func NewSortedIndex(batchSize int) Index {
	return &sortedIndex{
		entries:   make([]sortedEntry, 0),
		batchSize: batchSize,
	}
}

func (s *sortedIndex) Add(rowID uint32, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := sortedEntry{value: value, rowID: rowID}

	// Binary search for insertion
	pos := sort.Search(len(s.entries), func(i int) bool {
		return compareValues(s.entries[i].value, value) >= 0
	})

	// Insert into slice
	s.entries = append(s.entries, sortedEntry{})
	copy(s.entries[pos+1:], s.entries[pos:])
	s.entries[pos] = e
	return nil
}

func (s *sortedIndex) Remove(rowID uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Naive approach: find row by ID (could also store a map)
	for i, e := range s.entries {
		if e.rowID == rowID {
			// Remove it
			s.entries = append(s.entries[:i], s.entries[i+1:]...)
			break
		}
	}
	return nil
}

func (s *sortedIndex) Search(value interface{}) ([]uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// We'll find the first occurrence of `value` using a binary search
	// and then collect all adjacent entries with the same value.
	n := len(s.entries)
	if n == 0 {
		return nil, nil
	}

	// 1) Find left boundary via sort.Search
	left := sort.Search(n, func(i int) bool {
		return compareValues(s.entries[i].value, value) >= 0
	})
	if left == n {
		// No entries >= value
		return nil, nil
	}
	if compareValues(s.entries[left].value, value) != 0 {
		// The first entry >= value is not equal to value
		return nil, nil
	}

	// 2) Walk from left boundary to right to collect all matching
	result := make([]uint32, 0)
	for i := left; i < n && compareValues(s.entries[i].value, value) == 0; i++ {
		result = append(result, s.entries[i].rowID)
	}

	return result, nil
}

func (s *sortedIndex) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = make([]sortedEntry, 0)
	return nil
}

// compareValues is a helper that compares two interface{} values. Returns:
//
//	< 0 if a < b
//	= 0 if a == b
//	> 0 if a > b
func compareValues(a, b interface{}) int {
	switch va := a.(type) {
	case int:
		vb, ok := b.(int)
		if !ok {
			// Fallback to string compare of fmt.Sprintf
			return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		}
		return va - vb
	case int64:
		vb, ok := b.(int64)
		if !ok {
			return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		}
		switch {
		case va < vb:
			return -1
		case va > vb:
			return 1
		default:
			return 0
		}
	case float64:
		vb, ok := b.(float64)
		if !ok {
			return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		}
		switch {
		case va < vb:
			return -1
		case va > vb:
			return 1
		default:
			return 0
		}
	case string:
		vb, ok := b.(string)
		if !ok {
			return strings.Compare(va, fmt.Sprintf("%v", b))
		}
		return strings.Compare(va, vb)
	default:
		// Fallback to string compare
		return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
	}
}

// ---------------------------------------------------------------------
// Utility: In some advanced cases, you might want to handle numeric
// conversions more cleanly or use a custom comparator interface.
// This is a minimal demonstration for common types.
// ---------------------------------------------------------------------
