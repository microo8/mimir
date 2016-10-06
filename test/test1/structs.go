/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import "time"

//Person ...
type Person struct {
	Name, Lastname string
	Age            int `index:"Age"`
	Addresses      []*address
	Degree         degree
	BirthDate      time.Time `index:"Birth"`
	ContractFile   []byte    `index:"Contract"`
}

type address struct {
	Street     string
	Number     int
	PostalCode string
	City       string `index:"AddressCity"`
}

type degree struct {
	Name     string
	Position bool
}
