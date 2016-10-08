/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import (
	"bytes"
	"math/rand"
	"os"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const DBPATH = "/tmp/mimir_test"

func TestOpen(t *testing.T) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB("/")
	if err == nil {
		t.Error("DB opened in not existing path")
	}
	db, err = OpenDB(DBPATH)
	if err != nil {
		t.Error(err)
	}
	err = db.Close()
	if err != nil {
		t.Error(err)
	}
	_, err = db.Persons.Add(&Person{Name: "a", Lastname: "b", Age: 34})
	if err == nil {
		t.Error("Added person to closed db")
	}
}

func TestAddGet(t *testing.T) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB(DBPATH)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	id, err := db.Persons.Add(&Person{Name: "a", Lastname: "b", Age: 34})
	if err != nil {
		t.Error(err)
	}
	if id == 0 {
		t.Error("id is zero")
	}
	p, err := db.Persons.Get(id)
	if err != nil {
		t.Error(err)
	}
	if p.Name != "a" || p.Lastname != "b" || p.Age != 34 {
		t.Error("getted person is not equal to the included")
	}
}

func TestAddUpdate(t *testing.T) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB(DBPATH)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	p := &Person{Name: "a", Lastname: "b", Age: 34}
	id, err := db.Persons.Add(p)
	if err != nil {
		t.Error(err)
	}
	if id == 0 {
		t.Fatal("id is zero")
	}
	p.Name = "c"
	err = db.Persons.Update(id, p)
	if err != nil {
		t.Fatal(err)
	}
	p, err = db.Persons.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "c" || p.Lastname != "b" || p.Age != 34 {
		t.Error("getted person is not equal to the included")
	}
}

func TestAddDelete(t *testing.T) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB(DBPATH)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	persons := db.Persons
	id, err := persons.Add(personObj)
	if err != nil {
		t.Error(err)
	}
	if id == 0 {
		t.Error("id is zero")
	}
	err = persons.Delete(id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = persons.Get(id)
	if err == nil {
		t.Error("Person deleted can't be Get back!")
	}
}

func TestLexDumpInt(t *testing.T) {
	data := []struct {
		a, b int
	}{
		{1, 0},
		{10000, 23},
		{100, -100},
		{-1, -23},
		{1123123123213123123, -2331231232131231231},
		{-1123123123213123123, -2331231232131231231},
	}
	for i, d := range data {
		if bytes.Compare(lexDumpInt(d.a), lexDumpInt(d.b)) != 1 {
			t.Errorf("in %d (%v) a >= b", i, d)
		}
	}
}

func TestLexDumpString(t *testing.T) {
	data := []struct {
		a, b string
	}{
		{"z", "y"},
		{"bla", "ameh"},
		{"xxxxxxxxxx", "aaaaaaaa"},
		{"Zagreb", "ZZZZZ"},
	}
	for i, d := range data {
		if bytes.Compare(lexDumpString(d.a), lexDumpString(d.b)) == -1 {
			t.Errorf("in %d (%v) a >= b", i, d)
		}
	}
}

func TestLexDumpID(t *testing.T) {
	for i := 0; i < 100000; i++ {
		d := rand.Int63()
		if lexLoadID(lexDumpID(d)) != d {
			t.Errorf("lexDumpID(%d) not equal", d)
		}
	}
}

type IterPersonID struct {
	id int64
	p  *Person
}

func TestIter(t *testing.T) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB(DBPATH)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	persons := []*IterPersonID{
		&IterPersonID{
			p: &Person{Name: "asd", Lastname: "asdas", Age: 12, Addresses: []*address{
				&address{Street: "tserew", Number: 123, City: "Amsterdam"},
			}},
		},
		&IterPersonID{
			p: &Person{Name: "foo", Lastname: "bar", Age: 123, Addresses: []*address{
				&address{Street: "apsdosadpsaojd", Number: 1232, City: "Berlin"},
			}},
		},
		&IterPersonID{
			p: &Person{Name: "meh", Lastname: "barbarbar", Age: 1234, Addresses: []*address{
				&address{Street: "Ble", Number: 222, City: "London"},
				&address{Street: "Bla", Number: 666, City: "Tokio/Japan"},
			}},
		},
	}

	for i, p := range persons {
		id, err := db.Persons.Add(p.p)
		if err != nil {
			t.Error(i, err)
		}
		p.id = id
		if p.id == 0 {
			t.Errorf("Person added and has 0 id")
		}
	}

	iter := db.Persons.All()
	defer iter.Release()
	num := 0
	for iter.Next() {
		_, err := iter.Value()
		if err != nil {
			t.Fatal(err)
		}
		if iter.ID() == 0 {
			t.Fatal("IterPersonAll id is 0")
		}
		num++
	}
	if num != len(persons) {
		t.Errorf("Iterator Person iterated %d times and included were %d", num, len(persons))
	}

	for i, p := range persons {
		iterIndex := db.Persons.AgeEq(p.p.Age)
		num = 0
		for iterIndex.Next() {
			person, err := iterIndex.Value()
			if err != nil {
				t.Error(err)
			}
			num++
			if person.Age != p.p.Age {
				t.Errorf("in %d Person age is not %d", i, p.p.Age)
			}
			if person.Name != p.p.Name {
				t.Errorf("in %d Person name is not %s", i, p.p.Name)
			}
			if person.Lastname != p.p.Lastname {
				t.Errorf("in %d Person lastname is not %s", i, p.p.Lastname)
			}
			if iterIndex.ID() != p.id {
				t.Errorf("Iterated persons id (%d) is not equal to added persons id (%d)", iterIndex.ID(), p.id)
			}
		}
		if num != 1 {
			t.Errorf("IterAge iterated %d times", num)
		}
		iterIndex.Release()
	}

	s := "0"
	iterIndex := db.Persons.AddressCityRange(&s, nil)
	i := 0
	indices := []int{0, 1, 2, 2}
	for iterIndex.Next() {
		p, err := iterIndex.Value()
		if err != nil {
			t.Error(err)
		}
		if p.Name != persons[indices[i]].p.Name {
			t.Errorf("Person name not equal in %d CityRange", i)
		}
		if iterIndex.ID() != persons[indices[i]].id {
			t.Errorf("Person id not equal in %d CityRange", i)
		}
		i++
	}
	iterIndex.Release()
	if i == 0 {
		t.Error("No iteration in IterAddressCityRange")
	}
	if i != 4 {
		t.Error("IterAddressCityRange not iterated trough all indices")
	}

	i = 0
	iterIndex = db.Persons.BirthEq(time.Time{})
	for iterIndex.Next() {
		_, err := iterIndex.Value()
		if err != nil {
			t.Error(err)
		}
		i++
	}
	if i != 3 {
		t.Errorf("BirthEq iter iterated %d times and not 3", i)
	}
	iterIndex.Release()
}

var personObj = &Person{Name: "meh", Lastname: "barbarbar", Age: 1234, Addresses: []*address{
	&address{Street: "Ble", Number: 222, City: "Tokio"},
	&address{Street: "Bla", Number: 666, City: "Hell"},
	&address{Street: "Bla", Number: 666, City: "Hell"},
	&address{Street: "Bla", Number: 666, City: "Hell"},
	&address{Street: "Bla", Number: 666, City: "Hell"},
	&address{Street: "Bla", Number: 666, City: "Hell"},
	&address{Street: "Bla", Number: 666, City: "Hell"},
}}

func BenchmarkInsertGet(b *testing.B) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB(DBPATH)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < b.N; i++ {
		id, err := db.Persons.Add(personObj)
		if err != nil {
			b.Fatal(err)
		}
		_, err = db.Persons.Get(id)
		if err != nil {
			b.Errorf("Getting person error: %s", err)
		}
	}
}

func BenchmarkInsertGetUpdate(b *testing.B) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB(DBPATH)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < b.N; i++ {
		id, err := db.Persons.Add(personObj)
		if err != nil {
			b.Fatal(err)
		}
		p, err := db.Persons.Get(id)
		if err != nil {
			b.Errorf("Getting person error: %s", err)
		}
		err = db.Persons.Update(id, p)
		if err != nil {
			b.Errorf("Update person error: %s", err)
		}
	}
}
