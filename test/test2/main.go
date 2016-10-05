package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/microo8/mimir/test/test2/mypackage"
)

func main() {
	log.SetFlags(log.Lshortfile)
	os.RemoveAll("dbb")
	db2, err := db.OpenDB("dbb", json.Marshal, json.Unmarshal)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	persons := db2.Persons()
	id, err := persons.Add(&db.Person{
		Name: "Foo",
		Age:  20,
	})

	if err != nil {
		panic(err)
	}

	person, err := persons.Get(id)
	if err != nil {
		panic(err)
	}
	log.Println("GET", person.Name, person.Age)

	person.Name = "Meh"
	err = persons.Update(id, person)
	if err != nil {
		panic(err)
	}

	iter := persons.All()
	defer iter.Release()
	for iter.Next() {
		p, err := iter.Value()
		if err != nil {
			log.Fatalln("iter value:", err)
		}
		log.Println(p.Name, p.Age)
	}
}
