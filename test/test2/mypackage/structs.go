package db

//Person ...
type Person struct {
	Name string
	Age  int `index:"Age"`
}
