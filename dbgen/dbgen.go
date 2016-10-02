package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/microo8/meh/dbgen"
)

var (
	output = flag.String("o", "db.go", "Output file")
)

func main() {
	flag.Parse()
	if len(flag.Args()) != 1 {
		flag.Usage()
		return
	}
	if *output == "db.go" {
		*output = path.Join(path.Dir(flag.Arg(0)), "db.go")
	}
	gen, err := dbgen.Parse(flag.Arg(0))
	if err != nil {
		fmt.Printf("Parse error: %s", err)
		return
	}
	fmt.Println(gen)
	file, err := os.Create(*output)
	if err != nil {
		fmt.Printf("Cannot open file: %s", err)
		return
	}
	defer file.Close()
	err = gen.Generate(file)
	if err != nil {
		fmt.Printf("Generate error: %s", err)
	}
}
