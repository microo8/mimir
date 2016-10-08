//Package main genereated with github.com/microo8/mimir DO NOT MODIFY!
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

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

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

func lexDumpUint(v int) []byte {
	return lexDumpUint64(uint64(v))
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

func lexDumpInt8(v int8) []byte {
	return lexDumpInt64(int64(v))
}

func lexDumpInt16(v int16) []byte {
	return lexDumpInt64(int64(v))
}

func lexDumpInt32(v int32) []byte {
	return lexDumpInt64(int64(v))
}

func lexDumpUint8(v uint8) []byte {
	return lexDumpUint64(uint64(v))
}

func lexDumpUint16(v int16) []byte {
	return lexDumpUint64(uint64(v))
}

func lexDumpUint32(v int32) []byte {
	return lexDumpUint64(uint64(v))
}

func lexDumpFloat32(v float32) []byte {
	return lexDumpUint64(uint64(math.Float32bits(v)))
}

func lexDumpFloat64(v float64) []byte {
	return lexDumpUint64(math.Float64bits(v))
}

func lexDumpByte(v byte) []byte {
	return []byte{v}
}

func lexDumpRune(v rune) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func lexDumpBytes(v []byte) []byte {
	return v
}

func lexDumpRunes(v []rune) []byte {
	return []byte(string(v))
}

func lexDumpTime(v time.Time) []byte {
	unix := v.Unix()
	nano := int64(v.Nanosecond())
	ret := lexDumpInt64(unix)
	ret = append(ret, lexDumpInt64(nano)...)
	return ret
}

func lexDumpID(id int64) []byte {
	return []byte{byte(id >> 56), byte(id >> 48), byte(id >> 40), byte(id >> 32), byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id)}
}

func lexLoadID(idBytes []byte) int64 {
	var id int64
	for _, t := range idBytes {
		id = (id << 8) | int64(^t)
	}
	return ^id
}

//DB handler to the db
type DB struct {
	db *leveldb.DB

	Persons *PersonCollection
}

//OpenDB opens the database
func OpenDB(path string) (*DB, error) {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	db := new(DB)
	db.db = ldb

	db.Persons = &PersonCollection{db: db}

	return db, nil
}

//Close closes the database
func (db *DB) Close() error {
	return db.db.Close()
}

//Iter implemenst basic iterator functions
type Iter struct {
	it iterator.Iterator
}

//ID returns id of current object
func (it *Iter) ID() int64 {
	key := it.it.Key()
	return lexLoadID(key[len(key)-8:])
}

//Next sets the iterator to the next object, or returns false
func (it *Iter) Next() bool {
	return it.it.Next()
}

//Release closes the iterator
func (it *Iter) Release() {
	it.it.Release()
}

//PersonCollection represents the collection of Persons
type PersonCollection struct {
	db *DB
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
	err := json.Unmarshal(data, &obj)
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
	key = append([]byte("Person/"), key[len(key)-8:]...)
	data, err := it.col.db.db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	var obj Person
	err = json.Unmarshal(data, &obj)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

//Get returns Person with specified id or an error
func (col *PersonCollection) Get(id int64) (*Person, error) {
	data, err := col.db.db.Get(append([]byte("Person/"), lexDumpID(id)...), nil)
	if err != nil {
		return nil, err
	}
	var obj Person
	err = json.Unmarshal(data, &obj)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

//Add inserts new Person to the db
func (col *PersonCollection) Add(obj *Person) (int64, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return 0, fmt.Errorf("json encoding struct Person error: %s", err)
	}
	batch := new(leveldb.Batch)
	id := rand.Int63()
	key := append([]byte("Person/"), lexDumpID(id)...)
	batch.Put(key, data)

	err = col.addIndex([]byte("$Person/"), batch, id, obj)
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
func (col *PersonCollection) Update(id int64, obj *Person) error {
	key := append([]byte("Person/"), lexDumpID(id)...)

	oldObj, err := col.Get(id)
	if err != nil {
		return fmt.Errorf("Person with id (%d) doesn't exist: %s", id, err)
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("json encoding struct Person error: %s", err)
	}
	batch := new(leveldb.Batch)
	batch.Put(key, data)

	err = col.removeIndex([]byte("$Person/"), batch, id, oldObj)
	if err != nil {
		return err
	}
	err = col.addIndex([]byte("$Person/"), batch, id, obj)
	if err != nil {
		return err
	}

	err = col.db.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

//Delete removes Person from the db with specified id
func (col *PersonCollection) Delete(id int64) error {
	key := append([]byte("Person/"), lexDumpID(id)...)

	oldObj, err := col.Get(id)
	if err != nil {
		return nil
	}

	batch := new(leveldb.Batch)
	batch.Delete(key)

	err = col.removeIndex([]byte("$Person/"), batch, id, oldObj)
	if err != nil {
		return fmt.Errorf("Removing Person error: %s", err)
	}

	err = col.db.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

//All returns an iterator witch iterates trough all Persons
func (col *PersonCollection) All() *IterPerson {
	return &IterPerson{
		Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix([]byte("Person/")), nil)},
		col:  col,
	}
}

//AddressCityEq iterates trough Person AddressCity index with equal values
func (col *PersonCollection) AddressCityEq(val string) *IterIndexPerson {
	valDump := lexDumpString(val)
	prefix := append([]byte("$Person/AddressCity/"), valDump...)
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix(prefix), nil)},
			col:  col,
		},
	}
}

//AddressCityRange iterates trough Person AddressCity index in the specified range
func (col *PersonCollection) AddressCityRange(start, limit *string) *IterIndexPerson {
	startDump := []byte("$Person/AddressCity/")
	if start != nil {
		startDump = append(startDump, lexDumpString(*start)...)
	}
	var limitDump []byte
	if limit != nil {
		limitDump = append([]byte("$Person/AddressCity/"), lexDumpString(*limit)...)
	} else {
		limitDump = util.BytesPrefix([]byte("$Person/AddressCity/")).Limit
	}
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(&util.Range{
				Start: startDump,
				Limit: limitDump,
			}, nil)},
			col: col,
		},
	}
}

//AgeEq iterates trough Person Age index with equal values
func (col *PersonCollection) AgeEq(val int) *IterIndexPerson {
	valDump := lexDumpInt(val)
	prefix := append([]byte("$Person/Age/"), valDump...)
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix(prefix), nil)},
			col:  col,
		},
	}
}

//AgeRange iterates trough Person Age index in the specified range
func (col *PersonCollection) AgeRange(start, limit *int) *IterIndexPerson {
	startDump := []byte("$Person/Age/")
	if start != nil {
		startDump = append(startDump, lexDumpInt(*start)...)
	}
	var limitDump []byte
	if limit != nil {
		limitDump = append([]byte("$Person/Age/"), lexDumpInt(*limit)...)
	} else {
		limitDump = util.BytesPrefix([]byte("$Person/Age/")).Limit
	}
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(&util.Range{
				Start: startDump,
				Limit: limitDump,
			}, nil)},
			col: col,
		},
	}
}

//BirthEq iterates trough Person Birth index with equal values
func (col *PersonCollection) BirthEq(val time.Time) *IterIndexPerson {
	valDump := lexDumpTime(val)
	prefix := append([]byte("$Person/Birth/"), valDump...)
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix(prefix), nil)},
			col:  col,
		},
	}
}

//BirthRange iterates trough Person Birth index in the specified range
func (col *PersonCollection) BirthRange(start, limit *time.Time) *IterIndexPerson {
	startDump := []byte("$Person/Birth/")
	if start != nil {
		startDump = append(startDump, lexDumpTime(*start)...)
	}
	var limitDump []byte
	if limit != nil {
		limitDump = append([]byte("$Person/Birth/"), lexDumpTime(*limit)...)
	} else {
		limitDump = util.BytesPrefix([]byte("$Person/Birth/")).Limit
	}
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(&util.Range{
				Start: startDump,
				Limit: limitDump,
			}, nil)},
			col: col,
		},
	}
}

//ContractEq iterates trough Person Contract index with equal values
func (col *PersonCollection) ContractEq(val []byte) *IterIndexPerson {
	valDump := lexDumpBytes(val)
	prefix := append([]byte("$Person/Contract/"), valDump...)
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix(prefix), nil)},
			col:  col,
		},
	}
}

//ContractRange iterates trough Person Contract index in the specified range
func (col *PersonCollection) ContractRange(start, limit *[]byte) *IterIndexPerson {
	startDump := []byte("$Person/Contract/")
	if start != nil {
		startDump = append(startDump, lexDumpBytes(*start)...)
	}
	var limitDump []byte
	if limit != nil {
		limitDump = append([]byte("$Person/Contract/"), lexDumpBytes(*limit)...)
	} else {
		limitDump = util.BytesPrefix([]byte("$Person/Contract/")).Limit
	}
	return &IterIndexPerson{
		IterPerson{
			Iter: &Iter{col.db.db.NewIterator(&util.Range{
				Start: startDump,
				Limit: limitDump,
			}, nil)},
			col: col,
		},
	}
}

func (col *PersonCollection) addIndex(prefix []byte, batch *leveldb.Batch, id int64, obj *Person) (err error) {

	if obj == nil {
		return nil
	}
	//TODO check if this struct has index than buf and idDump doesn't have to be here
	var buf bytes.Buffer
	idDump := lexDumpID(id)

	for _, attr := range obj.Addresses {

		err = col.db.addaddressIndex(prefix, batch, id, attr)

		if err != nil {
			return err
		}
	}

	buf.Reset()
	buf.Write(prefix)
	buf.WriteString("Age/")
	buf.Write(lexDumpInt(obj.Age))
	buf.WriteRune('/')
	buf.Write(idDump)
	batch.Put(buf.Bytes(), nil)

	buf.Reset()
	buf.Write(prefix)
	buf.WriteString("Birth/")
	buf.Write(lexDumpTime(obj.BirthDate))
	buf.WriteRune('/')
	buf.Write(idDump)
	batch.Put(buf.Bytes(), nil)

	for _, attr := range obj.ContractFile {
		buf.Reset()
		buf.Write(prefix)
		buf.WriteString("Contract/")
		buf.Write(lexDumpByte(attr))
		buf.WriteRune('/')
		buf.Write(idDump)
		batch.Put(buf.Bytes(), nil)
	}

	return nil
}

func (col *PersonCollection) removeIndex(prefix []byte, batch *leveldb.Batch, id int64, obj *Person) (err error) {

	if obj == nil {
		return nil
	}
	var buf bytes.Buffer
	idDump := lexDumpID(id)

	for _, attr := range obj.Addresses {

		err = col.db.removeaddressIndex(prefix, batch, id, attr)

		if err != nil {
			return err
		}
	}

	buf.Reset()
	buf.Write(prefix)
	buf.WriteString("Age/")
	buf.Write(lexDumpInt(obj.Age))
	buf.WriteRune('/')
	buf.Write(idDump)
	batch.Delete(buf.Bytes())

	buf.Reset()
	buf.Write(prefix)
	buf.WriteString("Birth/")
	buf.Write(lexDumpTime(obj.BirthDate))
	buf.WriteRune('/')
	buf.Write(idDump)
	batch.Delete(buf.Bytes())

	for _, attr := range obj.ContractFile {
		buf.Reset()
		buf.Write(prefix)
		buf.WriteString("Contract/")
		buf.Write(lexDumpByte(attr))
		buf.WriteRune('/')
		buf.Write(idDump)
		batch.Delete(buf.Bytes())
	}

	return nil
}

func (db *DB) addaddressIndex(prefix []byte, batch *leveldb.Batch, id int64, obj *address) (err error) {

	if obj == nil {
		return nil
	}
	//TODO check if this struct has index than buf and idDump doesn't have to be here
	var buf bytes.Buffer
	idDump := lexDumpID(id)

	buf.Reset()
	buf.Write(prefix)
	buf.WriteString("AddressCity/")
	buf.Write(lexDumpString(obj.City))
	buf.WriteRune('/')
	buf.Write(idDump)
	batch.Put(buf.Bytes(), nil)

	return nil
}

func (db *DB) removeaddressIndex(prefix []byte, batch *leveldb.Batch, id int64, obj *address) (err error) {

	if obj == nil {
		return nil
	}
	var buf bytes.Buffer
	idDump := lexDumpID(id)

	buf.Reset()
	buf.Write(prefix)
	buf.WriteString("AddressCity/")
	buf.Write(lexDumpString(obj.City))
	buf.WriteRune('/')
	buf.Write(idDump)
	batch.Delete(buf.Bytes())

	return nil
}
