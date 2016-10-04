[![Report card](http://goreportcard.com/badge/microo8/mimir)](http://goreportcard.com/report/microo8/mimir)

# Mímir
Generates code for an embedded database from structs in golang with minimal API.

When you trying to make a little tool that must store some objects somewhere, it is hard to make an easy to use store quickly.
That's why `Mímir` was created! It takes structs from an go file and generates code to store, retrieve and iterate trough collections of objects defined by parsed structs.
The store operates on top of [leveldb](https://github.com/syndtr/goleveldb). The name Mímir is from the Norse mythology, [Mímir](https://en.wikipedia.org/wiki/M%C3%ADmir) a wisdom deity.

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
//Get the Persons Collection
persons := db.Persons()

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
for iter.Next() {
	p := iter.Value()
	fmt.Println(p.Name, p.Lastname, p.Age)
}
```

All exported structs get there own collection. The struct tags can be used as index declarations.
In the example above `Person.Age` has an index with name `"Age"` and `address.City` has index `"AddressCity"`.
All substructs with specified indexes build also an index for the collection above. So `"AddressCity"` is an index for Persons.

```go
//Iterating trough persons with Age in range 30-40
iter := persons.IterAgeRange(30, 40)
for iter.Next() {
	p := iter.Value()
	fmt.Println(p.Name, p.Lastname, p.Age)
}

//Iterating trough persons whitch have city in address equal to "London"
iter := persons.IterAddressCityEq("London")
for iter.Next() {
	p := iter.Value()
	fmt.Println(p.Name, p.Lastname, p.Age)
}
```
