/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/microo8/mimir"
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
	gen, err := mimir.Parse(flag.Arg(0))
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
