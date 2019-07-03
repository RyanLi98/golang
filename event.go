package event

//go:generate msgp
// Populate current state object in rewind, without having to go forward after
// Restful paths equivalent to heirarchal level (not necessarily JSON in event store)
// DEPRICATED: For table T, use TDRCMulti (contains) rather than Ts (plural)
// AR - Aggregate root (think bucket, entity, e.g. policy)
// An AR has a schema (a type) that is known at compile-time, and can be represented as JSON
// Events consist of a subset of the full AR schema

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/boltdb/bolt" // TODO: consider alternatives
	"github.com/imdario/mergo"
)

// Map feeds the mspg generator
type Map map[string]interface{}

type Etype byte

const (
	Empty    Etype = 'E' // no data
	Snapshot Etype = 'S' // an indication, quote, or policy
	Delta    Etype = 'D' // one or more field changes
	JSON     Etype = 'J' // json field change(s)
	Msgp     Etype = 'M' // json field change(s)
	Command  Etype = 'C' // e.g. "New", "Rate, "Save", "Cancel", etc.
)

// Event lives in a bucket named: "P" for policy, "Q" for quote, "I" for indication followed by a number e.g. P003432
// each bucket is sorted by event date
// TODO: consider adding these to events
// UserID    int       // for auditing
// Version   uint64    // handle concurrency conflicts and partial connection scenarios [http://danielwhittaker.me/2014/09/29/handling-concurrency-issues-cqrs-event-sourced-system/]
// ProcessId int       // tie a series of events to a command: snapshot=-1
// BlockChainHASH
type Event struct {
	Type    Etype
	Entered time.Time   // Go time is monotonic: https://golang.org/pkg/time/#pkg-overview
	Value   interface{} // Delta:string  Snapshot:map[string]interface{} JSON:[]byte
}

var (
	db             *bolt.DB                // event store
	snapThreashold = time.Millisecond * 30 // unguarded

	mxLastUpdate sync.Mutex // guards lastUpdate
	lastUpdate   int64      // prevent duplicate rows (t.UnixNano())

	zeroTime time.Time
	debug    int = 0 // 0=off 1=some 2=verbose
)

func init() {
	snapping = make(map[string]bool)
}

// SetSnapThreashold assigns the snapshot threshhold
func SetSnapThreashold(d time.Duration) error {
	if d >= time.Millisecond*400 {
		return fmt.Errorf("Set SnapThreashold to a duration less than %v", d)
	}
	snapThreashold = d
	return nil
}

// OpenDB initialize database
func OpenDB(filename string) (err error) {
	db, err = bolt.Open(filename, 0600, nil)
	if err != nil && db == nil {
		err = fmt.Errorf("db is nil")
	}
	return
}

// Update stores an Event in a given bucket keyed to a particular time
// t: actual time of event e.g. in the past (defaults to time.Now())
// Time key must be unique for the bucket; two events can't be at exactly the same time
// Updates are a snapshot, which is a complete record,
// or a delta writing one or more specific fields,
// or a command
// Safe to call from goroutines
func (e *Event) Update(bucket string, occur *time.Time) error {
	if bucket == "" {
		return fmt.Errorf("Bucket missing")
	}
	var (
		b       *bolt.Bucket
		timekey int64 // key to events in the (sorted) bucket
	)
	e.Entered = time.Now()
	if occur != nil {
		if occur.UnixNano() > e.Entered.UnixNano() {
			return fmt.Errorf("cannot Update to future time %v", occur)
		}
		// event is sometime in the past, possibly out-of-sequence
		timekey = occur.UnixNano()

		// disallow Update if there is an existing record at this time
		err := db.View(func(tx *bolt.Tx) error {
			b = tx.Bucket([]byte(bucket))
			if b == nil {
				return nil // No bucket yet. Ok, to write first record in new bucket.
			}
			if b.Get(itob(timekey)) != nil {
				// e.g. t == e.Entered?
				// TODO: Find an available time close to t???
				return fmt.Errorf("event already exists in bucket %q at %v", bucket, occur)
			}
			return nil
		})
		if err != nil {
			return err
		}

	} else {
		// event is now!
		// lastUpdate preserves unique keys based on time.
		// lastUpdate is across all buckets, so this approach to uniqueness puts a 10k/ms ceiling on updates.
		// TODO: test the assumption that this is rfaster than just calling b.Get()
		timekey = e.Entered.UnixNano()
		mxLastUpdate.Lock() // avoid race on lastUpdate
		if lastUpdate >= timekey {
			// advance timekey to just after lastUpdate
			// (UnixNano ticks are 100ns each)
			timekey = lastUpdate + 1
		}
		lastUpdate = timekey
		mxLastUpdate.Unlock()
	}

	// Marshal event into msgp
	var buf []byte
	buf, err := e.MarshalMsg(buf) // generated type 'Event' encoder
	if err != nil {
		return fmt.Errorf("EncodeMsg: %v", err)
	}
	// buf now contains msgp encoding of event
	err = db.Update(func(tx *bolt.Tx) error {
		b, err = tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		// TODO: this needs to be implemented on command level, not individual events
		// Unique sequential ID -- use to detect race conditions in business logic
		// e.Version, _ = b.NextSequence()

		if debug > 1 {
			fmt.Printf("** Put %s Entered=%d %s\n", bucket, e.Entered.UnixNano(), string(buf))
		}
		// big endian times will sort as expected
		return b.Put(itob(timekey), buf)
	})
	return err // happy path err is nil
}

// CurrentState walks events to find the current state of given bucket.
// Returns a map object suitable for marshaling JSON
// It looks backwards from latest event for a snapshot or the beginning.
// It then reads forward combining individual events into a heirarchical record.
// When fetching CurrentState longer than n (e.g. 30ms) a snapshot is taken.
// Safe to call from goroutines
// Varying n allows us to trade speed for space yielding "constant time" O(1) performance.
//         ----- aggregation times -------
// records  w/out snapshot  with snapshot
//          O(n)            O(1)
// ------- ---------------- -------------
// 10000k : 300.0 ms/op     0.183 ms/op
//  1000k :  30.33 ms/op    0.184 ms/op
//   100k :   3.00 ms/op    0.181 ms/op
//    10k :   0.305 ms/op   0.133 ms/op
//     1k :   0.032 ms/op   0.033 ms/op
//
// 0.183 ms/op is 5.46 op/ms, e.g. 5460 op/s, ~328,000 op/min
func CurrentState(bucket string, snapit bool) (map[string]interface{}, error) {
	var state Event

	// declare vars outside loop to reduce allocs
	var (
		err     error
		k, v    []byte
		lastKey []byte
		t0      = time.Now()
		dirty   bool
	)

	snapshotBucket := append([]byte(bucket), 'S')
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("not found: bucket %s ", bucket)
		}
		c := b.Cursor()

		ssb := tx.Bucket(snapshotBucket)
		if ssb != nil {
			// restore from most recent Snapshot in this Policy, Quote, Indication (bucketS)
			ssk, ssv := ssb.Cursor().Last()
			_, err = state.UnmarshalMsg(ssv)
			if err != nil {
				return fmt.Errorf("CurrentState: %v", err)
			}
			if state.Type != Snapshot {
				return fmt.Errorf("Expected a Snapshot")
			}
			if _, ok := state.Value.(map[string]interface{}); !ok {
				return fmt.Errorf("Expected a Snapshot value of type map[string]interface{}; got %T", state.Value)
			}
			// move event cursor to first event after stapshot
			k, v = c.Seek(ssk)
		} else {
			// no snapshot found -- initialize state from events
			state.Value = make(map[string]interface{})
			k, v = c.First()
		}
		if k == nil {
			return nil // no new events
		}
		// cursor points to First() event, or first event after latest snapshot time-key -- Seek(ssk)
		if debug > 1 {
			fmt.Println("** retrieve current state of", bucket)
		}
		// walk events since last snapshot to restore state
		var event Event
		for ; k != nil; k, v = c.Next() {
			if len(v) == 0 {
				continue
			}
			if debug > 1 {
				fmt.Println("** kv", string(k), string(v))
			}
			_, err = event.UnmarshalMsg(v)
			if err != nil {
				return err
			}
			if debug > 1 {
				fmt.Printf("** cs:k=%d v=%+v\n", btoi(k), event.Value)
			}
			switch event.Type {
			case Delta:
				UpdateState(state.Value.(map[string]interface{}), event.Value.(string))
				dirty = true
			case JSON:
				m1, m2 := state.Value.(map[string]interface{}), event.Value.(map[string]interface{})
				err = mergo.MergeWithOverwrite(&m1, m2)
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(state.Value, m1) {
					fmt.Println("not deep equal")
				}
				dirty = true
			default:
				panic(fmt.Sprintf("!!! e.Type %v", event.Type)) // TODO Commands
			}
		}
		lastKey, _ = c.Last()
		return nil
	})
	if err != nil {
		return nil, err
	}
	if dirty && (snapit || time.Since(t0) > snapThreashold) {
		// ignore errors (snapshots are redundant)
		state.createSnapshot(string(snapshotBucket), time.Unix(0, btoi(lastKey)).Add(time.Nanosecond))
	}
	if debug > 1 {
		dumpBucket(state.Value.(map[string]interface{}), bucket, "")
		//fmt.Println("bucket:", bucket, state.Value)
	}
	return state.Value.(map[string]interface{}), nil
}

func readSnapshot(snapshotBucket []byte, tx *bolt.Tx) (state *Event, ssk []byte, err error) {
	var ssv []byte
	ssb := tx.Bucket(snapshotBucket)
	state = new(Event)
	if ssb == nil {
		return nil, nil, fmt.Errorf("readSnapshot: bucket not found %s", string(snapshotBucket))
	}
	ssk, ssv = ssb.Cursor().Last() // most recent snapshot in this Policy, Quote, Indication (bucketS)
	ssv, err = state.MarshalMsg(ssv)
	if err != nil {
		return nil, nil, fmt.Errorf("CurrentState: %v", err)
	}
	if state.Type != Snapshot {
		return nil, nil, fmt.Errorf("Expected a Snapshot")
	}
	if _, ok := state.Value.(map[string]interface{}); !ok {
		return nil, nil, fmt.Errorf("Expected a Snapshot value of type map[string]interface{}; got %T", state.Value)
	}
	return state, ssk, nil
}

var snapMx sync.Mutex        //guards snapping
var snapping map[string]bool // a snapshot is being saved (don't make another)

// snap adds a complete record of the bucket in a single event
// we never have to read earlier in the events than a snapshot
func (e *Event) createSnapshot(snapshotBucket string, t time.Time) error {
	//fmt.Printf("Create snapshot %q!!\n", bucket)
	if snapping == nil {
		// first time
	}
	snapMx.Lock()
	defer snapMx.Unlock()
	if _, ok := snapping[snapshotBucket]; ok {
		return fmt.Errorf("snap: snapshot already in progress")
	}
	snapping[snapshotBucket] = true
	e.Type = Snapshot
	err := e.Update(snapshotBucket, &t)
	if err != nil {
		return fmt.Errorf("snap: %v", err)
	}
	delete(snapping, snapshotBucket)
	return nil
}

// UpdateState writes a small state change (e.g. from web page changes)
// URI represents the heirarchal position of data
// delta elements are guaranteed to be unique
// delta e.g.  {"/P000001/Designee/2/Address":"7749 Maple Trail Rd","/P000001/Designee/2/ZipCode1":"20854"}
// TODO: Possibly alter UpdateState so it works with *Policy rather than map[string]interface{}
// Reflection? or Generate custom path parser with switch that walks up structs
func UpdateState(m map[string]interface{}, delta string) (string, error) {
	// parse all deltas
	var f interface{}
	err := json.Unmarshal([]byte(delta), &f)
	if err != nil {
		fmt.Println("json.Unmarshal error: ", err)
		return "", err
	} else if _, ok := f.(map[string]interface{}); !ok {
		return "", fmt.Errorf("delta is not an object")
	}

	// iterate each change
	var elem []string
	for path, v := range f.(map[string]interface{}) {
		elem = strings.Split(path, "/")     // e.g. [nil P000123 objName 2 fieldName]
		err := recurseUpdate(m, elem, 2, v) // start with elem[2]
		if err != nil {
			return "", err
		}
	}
	if len(elem) < 2 {
		return "", fmt.Errorf("path lacks sufficient elements %q", strings.Join(elem, "/"))
	}
	return elem[1], nil
}

// recurseUpdate crawls up the heirarchy
// elem is encoded path to follow in m to set value v
// i is current elem index
// if a node is nil recurseUpdate makes it
func recurseUpdate(m map[string]interface{}, elem []string, i int, v interface{}) error {
	if len(elem) <= i+1 {
		m[elem[i]] = v // leaf
		return nil
	}

	// lookahead to see if next elem in an array
	if '0' <= elem[i+1][0] && elem[i+1][0] <= '9' {
		// child is an array of objects
		n, err := strconv.ParseInt(elem[i+1], 10, 32)
		if err != nil {
			return fmt.Errorf("RecurseUpdate failed parsing int: %q" + elem[i+1])
		}
		if m[elem[i]] == nil {
			// init array up to n
			a := make([]map[string]interface{}, n)
			for k := 0; k < int(n); k++ {
				a[k] = make(map[string]interface{})
			}
			m[elem[i]] = a
		}
		if len(m[elem[i]].([]map[string]interface{})) <= int(n) {
			// add more elements
			a := m[elem[i]].([]map[string]interface{})
			for len(a) <= int(n) {
				a = append(a, make(map[string]interface{}))
			}
			m[elem[i]] = a
		}
		if m[elem[i]].([]map[string]interface{})[n] == nil { // init array element?
			m[elem[i]].([]map[string]interface{})[n] = make(map[string]interface{})
		}
		// skip over the already processed array number elem
		recurseUpdate(m[elem[i]].([]map[string]interface{})[n], elem, i+2, v)
	} else {
		// child object
		if m[elem[i]] == nil { // init object?
			m[elem[i]] = make(map[string]interface{})
		}
		recurseUpdate(m[elem[i]].(map[string]interface{}), elem, i+1, v)
	}
	return nil
}

// itob returns an 8-byte big endian representation (significant bytes first).
func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// inverse of itob()
func btoi(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

type Match struct {
	Bucket string
	Match  string
}

// BucketSearch returns matches to one or more strings
// It does full text search of the entire db which takes linear time: O(n).
func BucketSearch(search []string) ([]Match, error) {
	var found []Match
	rxBlank := regexp.MustCompile(`^\s*$`)
	// TODO: this might be broken up into several goroutines in separate boltdb transactions (tx)
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if string(name) == "NextNumber" {
				return nil
			}
			if debug > 0 {
				fmt.Println("BucketSearch:", string(name))
			}
			m, err := CurrentState(string(name), false)
			if err != nil {
				return err
			}
			j, err := json.Marshal(m)
			if err != nil {
				return err
			}

			// search may contain 2 or more strings
			// each string in search must match
			jj := string(j)
			var part []string
			for _, srch := range search {
				if rxBlank.MatchString(srch) {
					continue
				}
				if n := strings.Index(jj, srch); n > -1 {
					// seek for left and right commas
					var s, e int
					for s = n; jj[s] != ',' && jj[s] != '{' && s > 0; s-- {
					}
					for e = n + len(srch); jj[e] != ',' && jj[e] != '}' && e < len(jj); e++ {
					}
					part = append(part, strings.Replace(jj[s+1:e], "\"", "", 2))
				} else {
					// failed a string
					return nil
				}
			}
			found = append(found, Match{Bucket: string(name), Match: strings.Join(part, ", ")})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

// NextNumber creates, increments and stores named counters
// elem: "Q" for quote, "P" for policy
func NextNumber(elem string) (uint64, error) {
	var n uint64

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("NextNumber"))
		if err != nil {
			return fmt.Errorf("CreateBucketIfNotExists: %q %v", "NextNumber", err)
		}
		buf := make([]byte, 4)
		if got := b.Get([]byte(elem)); got == nil {
			// first in a new series
			n = 1
		} else {
			// next number in series
			n, _ = binary.Uvarint(got)
		}

		// save number+1
	again:
		n++
		binary.PutUvarint(buf, n)
		b.Put([]byte(elem), buf)

		// Create new bucket e.g. Q000123
		newBucket := fmt.Sprintf("%s%06d", elem, n)
		fmt.Printf("Creating new bucket %s\n", newBucket)
		_, err = tx.CreateBucket([]byte(newBucket))
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				// retry with incremented number
				goto again
			}
			return fmt.Errorf("CreateBucket: %q %v", newBucket, err)
		}

		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// DumpBucket dumps bucket contents to stdout
func DumpBucket(bucket string) error {

	cs, err := CurrentState(bucket, false)
	if err != nil {
		return err
	}
	dumpBucket(cs, bucket, "")
	return nil
}

func dumpBucket(cs interface{}, bucket, indent string) {
	if indent == "" {
		fmt.Println("--- bucket:", bucket)
		defer fmt.Println("---")
	}
	switch t := cs.(type) {
	case []map[string]interface{}:
		for i, v := range t {
			fmt.Printf("%s[%d]\n", indent, i)
			dumpBucket(v, bucket, indent+"    ")
		}
	case map[string]interface{}:
		for k, v := range t {
			fmt.Printf("%s%q:", indent, k)
			if s, ok := v.(string); ok {
				fmt.Printf("%q\n", s)
			} else {
				fmt.Println()
				dumpBucket(v, bucket, indent+"    ")
			}
		}
	case string:
		fmt.Printf("%s%q\n", indent, t)
	default:
		fmt.Printf("%s%v (%[1]T)\n", indent, t)
	}
}
