//file genereated with github.com/microo8/dbgen DO NOT MODIFY!
package main

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

//Constants for int encoding
const (
	IntMin      = 0x80
	intMaxWidth = 8
	intZero     = IntMin + intMaxWidth
	intSmall    = IntMax - intZero - intMaxWidth
	IntMax      = 0xfd
)

//lexDump functions for encoding values to lexicographically ordered byte array
func lexDumpString(v string) []byte {
	return []byte(v)
}

func lexDumpBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func lexDumpInt(v int) []byte {
	return lexDumpInt64(int64(v))
}

func lexDumpInt64(v int64) []byte {
	if v < 0 {
		switch {
		case v >= -0xff:
			return []byte{IntMin + 7, byte(v)}
		case v >= -0xffff:
			return []byte{IntMin + 6, byte(v >> 8), byte(v)}
		case v >= -0xffffff:
			return []byte{IntMin + 5, byte(v >> 16), byte(v >> 8), byte(v)}
		case v >= -0xffffffff:
			return []byte{IntMin + 4, byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
		case v >= -0xffffffffff:
			return []byte{IntMin + 3, byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
		case v >= -0xffffffffffff:
			return []byte{IntMin + 2, byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
		case v >= -0xffffffffffffff:
			return []byte{IntMin + 1, byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
		default:
			return []byte{IntMin, byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
		}
	}
	return lexDumpUint64(uint64(v))
}

func lexDumpUint64(v uint64) []byte {
	switch {
	case v <= intSmall:
		return []byte{intZero + byte(v)}
	case v <= 0xff:
		return []byte{IntMax - 7, byte(v)}
	case v <= 0xffff:
		return []byte{IntMax - 6, byte(v >> 8), byte(v)}
	case v <= 0xffffff:
		return []byte{IntMax - 5, byte(v >> 16), byte(v >> 8), byte(v)}
	case v <= 0xffffffff:
		return []byte{IntMax - 4, byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	case v <= 0xffffffffff:
		return []byte{IntMax - 3, byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	case v <= 0xffffffffffff:
		return []byte{IntMax - 2, byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	case v <= 0xffffffffffffff:
		return []byte{IntMax - 1, byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	default:
		return []byte{IntMax, byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	}
}

func lexLoadInt(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("insufficient bytes to decode uvarint value")
	}
	length := int(b[0]) - intZero
	if length < 0 {
		length = -length
		remB := b[1:]
		if len(remB) < length {
			return 0, fmt.Errorf("insufficient bytes to decode uvarint value: %s", remB)
		}
		var v int
		// Use the ones-complement of each encoded byte in order to build
		// up a positive number, then take the ones-complement again to
		// arrive at our negative value.
		for _, t := range remB[:length] {
			v = (v << 8) | int(^t)
		}
		return ^v, nil
	}

	v, err := lexLoadUint(b)
	if err != nil {
		return 0, err
	}
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("varint %d overflows int64", v)
	}
	return int(v), nil
}

func lexLoadUint(b []byte) (uint, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("insufficient bytes to decode uvarint value")
	}
	length := int(b[0]) - intZero
	b = b[1:] // skip length byte
	if length <= intSmall {
		return uint(length), nil
	}
	length -= intSmall
	if length < 0 || length > 8 {
		return 0, fmt.Errorf("invalid uvarint length of %d", length)
	} else if len(b) < length {
		return 0, fmt.Errorf("insufficient bytes to decode uvarint value: %v", b)
	}
	var v uint
	// It is faster to range over the elements in a slice than to index
	// into the slice on each loop iteration.
	for _, t := range b[:length] {
		v = (v << 8) | uint(t)
	}
	return v, nil
}

//Encode is an function for encoding objects to bytes
type Encode func(interface{}) ([]byte, error)

//Decode is an function for decoding objects from bytes
type Decode func([]byte, interface{}) error

//DB handler to the db
type DB struct {
	db     *leveldb.DB
	encode Encode
	decode Decode
}

//OpenDB opens the database
func OpenDB(path string, encode Encode, decode Decode) (*DB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &DB{db: db, encode: encode, decode: decode}, nil
}

//Close closes the database
func (db *DB) Close() error {
	return db.db.Close()
}

type Iter struct {
	it iterator.Iterator
}

//ID returns id of object
func (it *Iter) ID() int {
	key := it.it.Key()
	index := bytes.LastIndexByte(key, '/')
	if index == -1 {
		return 0
	}
	objID, err := lexLoadInt(key[index+1:])
	if err != nil {
		return 0
	}
	return objID
}

//Next sets the iterator to the next object, or returns false
func (it *Iter) Next() bool {
	return it.it.Next()
}

//PersonCollection represents the collection of Persons
type PersonCollection struct {
	db *DB
}

//Persons returns the Persons collection
func (db *DB) Persons() *PersonCollection {
	return &PersonCollection{db: db}
}

//IterPerson iterates trough all Person in db
type IterPerson struct {
	*Iter
	col *PersonCollection
}

//Value returns the Person on witch is the iterator
func (it *IterPerson) Value() (*Person, error) {
	data := it.it.Value()
	var obj Person
	err := it.col.db.decode(data, &obj)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

//IterIndexPerson iterates trough an index for Person in db
type IterIndexPerson struct {
	IterPerson
}

//Value returns the Person on witch is the iterator
func (it *IterIndexPerson) Value() (*Person, error) {
	key := it.it.Key()
	index := bytes.LastIndexByte(key, '/')
	if index == -1 {
		return nil, fmt.Errorf("Index for Person has bad encoding")
	}
	id, err := lexLoadInt(key[index+1:])
	if err != nil {
		return nil, err
	}
	obj, err := it.col.Get(id)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func prefixPerson(id int) []byte {
	buf := bytes.NewBuffer([]byte("Person"))
	buf.WriteRune('/')
	buf.Write(lexDumpInt(id))
	return buf.Bytes()
}

//Get returns Person with specified id or an error
func (col *PersonCollection) Get(id int) (*Person, error) {
	data, err := col.db.db.Get(prefixPerson(id), nil)
	if err != nil {
		return nil, err
	}
	var obj Person
	err = col.db.decode(data, &obj)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

//Add inserts new Person to the db
func (col *PersonCollection) Add(obj *Person) (int, error) {
	data, err := col.db.encode(&obj)
	if err != nil {
		return 0, err
	}
	batch := new(leveldb.Batch)
	id := rand.Int()
	batch.Put(prefixPerson(id), data)
	err = col.db.Persons().addIndex([]byte("$Person"), batch, id, obj)
	if err != nil {
		return 0, err
	}
	err = col.db.db.Write(batch, nil)
	if err != nil {
		return 0, err
	}
	return id, nil
}

//Update updates Person with specified id
func (col *PersonCollection) Update(id int, obj *Person) error {
	key := prefixPerson(id)
	_, err := col.db.db.Get(key, nil)
	if err != nil {
		return err
	}
	data, err := col.db.encode(&obj)
	if err != nil {
		return err
	}
	batch := new(leveldb.Batch)
	batch.Put(key, data)
	err = col.removeIndex(batch, id)
	if err != nil {
		return err
	}
	err = col.addIndex([]byte("Person"), batch, id, obj)
	if err != nil {
		return err
	}
	err = col.db.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

func (col *PersonCollection) removeIndex(batch *leveldb.Batch, id int) error {
	iter := col.db.db.NewIterator(util.BytesPrefix([]byte("$Person")), nil)
	for iter.Next() {
		key := iter.Key()
		index := bytes.LastIndexByte(key, '/')
		if index == -1 {
			return fmt.Errorf("Index for Person has bad encoding")
		}
		objID, err := lexLoadInt(key[index+1:])
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		if id == objID {
			batch.Delete(key)
		}
	}
	iter.Release()
	return iter.Error()
}

//All returns an iterator witch iterates trough all Persons
func (col *PersonCollection) All() *IterPerson {
	return &IterPerson{
		Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix([]byte("Person")), nil)},
		col:  col,
	}
}

//IterAddressCityEq iterates trough Person AddressCity index with equal values
func (col *PersonCollection) IterAddressCityEq(val string) *IterIndexPerson {
	valDump := lexDumpString(val)
	prefix := append([]byte("$Person/AddressCity/"), valDump...)
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix(prefix), nil)},
			col:  col,
		},
	}
}

//IterAddressCityRange iterates trough Person AddressCity index in the specified range
func (col *PersonCollection) IterAddressCityRange(start, limit string) *IterIndexPerson {
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(&util.Range{
				Start: append([]byte("$Person/AddressCity/"), lexDumpString(start)...),
				Limit: append([]byte("$Person/AddressCity/"), lexDumpString(limit)...),
			}, nil)},
			col: col,
		},
	}
}

//IterAgeEq iterates trough Person Age index with equal values
func (col *PersonCollection) IterAgeEq(val int) *IterIndexPerson {
	valDump := lexDumpInt(val)
	prefix := append([]byte("$Person/Age/"), valDump...)
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix(prefix), nil)},
			col:  col,
		},
	}
}

//IterAgeRange iterates trough Person Age index in the specified range
func (col *PersonCollection) IterAgeRange(start, limit int) *IterIndexPerson {
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(&util.Range{
				Start: append([]byte("$Person/Age/"), lexDumpInt(start)...),
				Limit: append([]byte("$Person/Age/"), lexDumpInt(limit)...),
			}, nil)},
			col: col,
		},
	}
}

func (col *PersonCollection) addIndex(prefix []byte, batch *leveldb.Batch, id int, obj *Person) (err error) {

	if obj == nil {
		return nil
	}
	var buf *bytes.Buffer

	for _, attr := range obj.Addresses {

		err = col.db.addaddressIndex(prefix, batch, id, attr)

		if err != nil {
			return err
		}
	}

	buf = bytes.NewBuffer(prefix)
	buf.WriteRune('/')
	buf.WriteString("Age")
	buf.WriteRune('/')
	buf.Write(lexDumpInt(obj.Age))
	buf.WriteRune('/')
	buf.Write(lexDumpInt(id))
	batch.Put(buf.Bytes(), nil)

	return nil
}

func (db *DB) addaddressIndex(prefix []byte, batch *leveldb.Batch, id int, obj *address) (err error) {

	if obj == nil {
		return nil
	}
	var buf *bytes.Buffer

	buf = bytes.NewBuffer(prefix)
	buf.WriteRune('/')
	buf.WriteString("AddressCity")
	buf.WriteRune('/')
	buf.Write(lexDumpString(obj.City))
	buf.WriteRune('/')
	buf.Write(lexDumpInt(id))
	batch.Put(buf.Bytes(), nil)

	return nil
}
