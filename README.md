[![Report card](http://goreportcard.com/badge/microo8/mimir)](http://goreportcard.com/report/microo8/mimir)

# Mímir
<img src="https://raw.githubusercontent.com/microo8/mimir/master/logo.png" alt="Mímir logo" align="right"/>

Generates code for an embedded database with minimal API from structs in golang.

When you trying to make a little tool that has to save some objects somewhere, it's hard to make an easy to use store quickly. And in existing embedded databases you must handle objects as `map[string]interface{}`.

That's why `Mímir` was created! It takes structs from an go file and generates code to store, retrieve and iterate trough collections of objects defined by parsed structs.
So you can easily store your actual structs (and not `[]byte` or `map[string]interface{}`).

The store operates on top of [leveldb](https://github.com/syndtr/goleveldb). The name is from the Norse mythology, [Mímir](https://en.wikipedia.org/wiki/M%C3%ADmir) is a wisdom deity.

* Mímir isn't stable, need more testing
* TODOs: more structs examples to test, add indexing support for all buildin types + time.Time
* Contribution of all kind is welcome!

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

generate db code: `mimir structs.go`

Usage:

```go
//Open db file, codecs must be defined by you
db, err := OpenDB("/tmp/mimirdb", json.Marshal, json.Unmarshal)
if err != nil {
	panic(err)
}
defer db.Close()

//Get the Persons Collection
persons := db.Persons

//Add an person to db
id, err := persons.Add(&Person{
	Name: "Foo",
	Lastname: "Bar",
	Addresses: []*address{&address{Street: "Valhalla", Number: 404, City: "Asgard"}},
})
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

//Iterate trough the all persons in the Persons Collection
iter := persons.All()
defer iter.Release() //iter must be released
for iter.Next() {
	p, err := iter.Value()
	if err != nil {
    	panic(err)
	}
	fmt.Println(p.Name, p.Lastname, p.Age)
}
```

All exported structs get there own collection. The struct tags can be used as index declarations.
In the example above `Person.Age` has an index with name `"Age"` and `address.City` has index `"AddressCity"`.
All substructs with specified indexes build also an index for the collection above. So `"AddressCity"` is an index for Persons.

```go
//Iterating trough persons with Age in range 30-40
iter := db.Persons.AgeRange(30, 40)
defer iter.Release()
for iter.Next() {
	p, err := iter.Value()
	if err != nil {
    	panic(err)
	}
	fmt.Println(p.Name, p.Lastname, p.Age)
}
```

```go
//Iterating trough persons whitch have city in address equal to "London"
iter := db.Persons.AddressCityEq("London")
defer iter.Release()
for iter.Next() {
	p, err := iter.Value()
	if err != nil {
    	panic(err)
	}
	fmt.Println(p.Name, p.Lastname, p.Age)
}
```
