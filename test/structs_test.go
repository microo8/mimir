package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

const DBPATH = "/tmp/meh"

func TestOpen(t *testing.T) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB(DBPATH, json.Marshal, json.Unmarshal)
	if err != nil {
		t.Error(err)
	}
	err = db.Close()
	if err != nil {
		t.Error(err)
	}
	_, err = db.AddPerson(&Person{Name: "a", Lastname: "b", Age: 34})
	if err == nil {
		t.Error(err)
	}
}

func TestAddGet(t *testing.T) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB(DBPATH, json.Marshal, json.Unmarshal)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	id, err := db.AddPerson(&Person{Name: "a", Lastname: "b", Age: 34})
	if err != nil {
		t.Error(err)
	}
	if id == 0 {
		t.Error("id is zero")
	}
	p, err := db.GetPerson(id)
	if err != nil {
		t.Error(err)
	}
	if p.Name != "a" || p.Lastname != "b" || p.Age != 34 {
		t.Error("getted person is not equal to the included")
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
	}
	for i, d := range data {
		if bytes.Compare(lexDumpString(d.a), lexDumpString(d.b)) != 1 {
			t.Errorf("in %d (%v) a >= b", i, d)
		}
	}
}

type IterPersonID struct {
	id int
	p  *Person
}

func TestIter(t *testing.T) {
	os.RemoveAll(DBPATH)
	db, err := OpenDB(DBPATH, json.Marshal, json.Unmarshal)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	persons := []*IterPersonID{
		&IterPersonID{
			p: &Person{Name: "asd", Lastname: "asdas", Age: 12, Addresses: []*address{
				&address{Street: "tserew", Number: 123, City: "NY"},
			}},
		},
		&IterPersonID{
			p: &Person{Name: "foo", Lastname: "bar", Age: 123, Addresses: []*address{
				&address{Street: "apsdosadpsaojd", Number: 1232, City: "London"},
			}},
		},
		&IterPersonID{
			p: &Person{Name: "meh", Lastname: "barbarbar", Age: 1234, Addresses: []*address{
				&address{Street: "Ble", Number: 222, City: "Tokio"},
				&address{Street: "Bla", Number: 666, City: "Hell"},
			}},
		},
	}

	for i, p := range persons {
		id, err := db.AddPerson(p.p)
		if err != nil {
			t.Error(i, err)
		}
		p.id = id
		if p.id == 0 {
			t.Errorf("Person added and has 0 id")
		}
	}

	iter := db.IterPersonAll()
	num := 0
	for iter.Next() {
		_, err := iter.Value()
		if err != nil {
			t.Error(err)
		}
		if iter.ID() == 0 {
			t.Error("IterPersonAll id is 0")
		}
		num++
	}
	if num != len(persons) {
		t.Errorf("Iterator Person iterated %d times and included were %d", num, len(persons))
	}

	for i, p := range persons {
		iterIndex := db.IterPersonAgeEq(p.p.Age)
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
	}
}
