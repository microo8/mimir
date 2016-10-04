[![Report card](http://goreportcard.com/badge/microo8/dbgen)](http://goreportcard.com/report/microo8/dbgen)

# dbgen
Generates minimal embedded database from structs in golang

When you trying to make a little tool that must store some objects somewhere, it is hard to make an easy to use store quickly.
That's why `dbgen` was created! It takes structs from an go file and generates code to store, retrieve and iterate trough collections of objects defined by parsed structs. The store is operates on top of [leveldb]()

example structs (structs.go):

```go
package main

//Person ...
type Person struct {
	Name, Lastname string
	Age            int `index:"Age"`
	Addresses      []*address
}

type address struct {
	Street     string
	Number     int
	PostalCode string
	City       string `index:"AddressCity"`
}
```  

generate db code: `dbgen structs.go`

Usage:

```go
//Get an Collection
persons := db.Persons()

//Add an person to db
id, err := persons.Add(&Person{Name: "Foo", Lastname: "Bar"})
if err != nil {
    panic(err)
}

//Get the person by id
person, err := persons.Get(id)
if err != nil {
    panic(err)
}

//Update person by id
person.Name = "Meh"
err := persons.Update(id, person)
if err != nil {
    panic(err)
}

//Delete person by id
err := persons.Delete(id)
if err != nil {
    panic(err)
}
```
