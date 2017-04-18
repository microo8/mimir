/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

//Package mimir implemenst the AST parsing and code generation
package mimir

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"regexp"
	"strings"
	"text/template"
)

var (
	findIndex = regexp.MustCompile("index: *\"([_a-zA-Z]*)\"")
	//INDEXABLE is an array of type names whitch have supported lexDump
	INDEXABLE = map[string]string{
		"int": "Int", "int8": "Int8", "int16": "Int16", "int32": "Int32", "int64": "Int64",
		"uint": "Uint", "uint8": "Uint8", "uint16": "Uint16", "uint32": "Uint32", "uint64": "Uint64",
		"float32": "Float32", "float64": "Float64",
		"string": "String", "rune": "Rune", "byte": "Byte", "[]rune": "Runes", "[]byte": "Bytes",
		"time.Time": "Time",
	}
)

func lexType(typ string) string {
	lex, ok := INDEXABLE[typ]
	if !ok {
		fmt.Printf("Error: type %s is not indexable", typ)
		return ""
	}
	return lex
}

//DBTEMPLATE the main mart of the db source
const DBTEMPLATE = `//Package {{.PackageName}} genereated with github.com/microo8/mimir DO NOT MODIFY!
package {{.PackageName}}
{{$gen := .Structs}}
import (
	"encoding/json"
	"fmt"
	"math"
    "math/rand"
	"time"

    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/util"
    "github.com/syndtr/goleveldb/leveldb/iterator"
)

//Constants for int encoding
const (
	IntMin      = 0x80
	intMaxWidth = 8
	intZero     = IntMin + intMaxWidth
	intSmall    = IntMax - intZero - intMaxWidth
	IntMax		= 0xfd
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

//lexDump functions for encoding values to lexicographically ordered byte array
func lexDumpString(v string) []byte {
	return []byte(v)
}

func lexDumpBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func lexDumpInt(v int) []byte {
	return lexDumpInt64(int64(v))
}

func lexDumpUint(v int) []byte {
	return lexDumpUint64(uint64(v))
}

func lexDumpInt64(v int64) []byte {
	if v < 0 {
		switch {
		case v >= -0xff:
			return []byte{IntMin+7, byte(v)}
		case v >= -0xffff:
			return []byte{IntMin+6, byte(v>>8), byte(v)}
		case v >= -0xffffff:
			return []byte{IntMin+5, byte(v>>16), byte(v>>8), byte(v)}
		case v >= -0xffffffff:
			return []byte{IntMin+4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
		case v >= -0xffffffffff:
			return []byte{IntMin+3, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
		case v >= -0xffffffffffff:
			return []byte{IntMin+2, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),	byte(v>>8), byte(v)}
		case v >= -0xffffffffffffff:
			return []byte{IntMin+1, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
		default:
			return []byte{IntMin, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
		}
	}
	return lexDumpUint64(uint64(v))
}

func lexDumpUint64(v uint64) []byte {
	switch {
	case v <= intSmall:
		return []byte{intZero+byte(v)}
	case v <= 0xff:
		return []byte{IntMax-7, byte(v)}
	case v <= 0xffff:
		return []byte{IntMax-6, byte(v>>8), byte(v)}
	case v <= 0xffffff:
		return []byte{IntMax-5, byte(v>>16), byte(v>>8), byte(v)}
	case v <= 0xffffffff:
		return []byte{IntMax-4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
	case v <= 0xffffffffff:
		return []byte{IntMax-3, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
	case v <= 0xffffffffffff:
		return []byte{IntMax-2, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
	case v <= 0xffffffffffffff:
		return []byte{IntMax-1, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
	default:
		return []byte{IntMax, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
	}
}

func lexDumpInt8(v int8) []byte {
	return lexDumpInt64(int64(v))
}

func lexDumpInt16(v int16) []byte {
	return lexDumpInt64(int64(v))
}

func lexDumpInt32(v int32) []byte {
	return lexDumpInt64(int64(v))
}

func lexDumpUint8(v uint8) []byte {
	return lexDumpUint64(uint64(v))
}

func lexDumpUint16(v int16) []byte {
	return lexDumpUint64(uint64(v))
}

func lexDumpUint32(v int32) []byte {
	return lexDumpUint64(uint64(v))
}

func lexDumpFloat32(v float32) []byte {
	return lexDumpUint64(uint64(math.Float32bits(v)))
}

func lexDumpFloat64(v float64) []byte {
	return lexDumpUint64(math.Float64bits(v))
}

func lexDumpByte(v byte) []byte {
	return []byte{v}
}

func lexDumpRune(v rune) []byte {
	return []byte{byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
}

func lexDumpBytes(v []byte) []byte {
	return v
}

func lexDumpRunes(v []rune) []byte {
	return []byte(string(v))
}

func lexDumpTime(v time.Time) []byte {
	unix := v.Unix()
	nano := int64(v.Nanosecond())
	ret := lexDumpInt64(unix)
	ret = append(ret, lexDumpInt64(nano)...)
	return ret
}

func lexDumpID(id int64) []byte {
	return []byte{byte(id>>56), byte(id>>48), byte(id>>40), byte(id>>32), byte(id>>24), byte(id>>16), byte(id>>8), byte(id)}
}

func lexLoadID(idBytes []byte) int64 {
	var id int64
	for _, t := range idBytes {
		id = (id << 8) | int64(^t)
	}
	return ^id
}

//DB handler to the db
type DB struct {
    db     *leveldb.DB
    {{range $structName, $struct := $gen}}{{if $struct.Exported}}
    {{$structName}}s *{{$structName}}Collection
	{{end}}{{end}}
}

//OpenDB opens the database
func OpenDB(path string) (*DB, error) {
    ldb, err := leveldb.OpenFile(path, nil)
    if err != nil {
        return nil, err
    }
    db := new(DB)
	db.db = ldb
    {{range $structName, $struct := $gen}}{{if $struct.Exported}}
	db.{{$structName}}s = &{{$structName}}Collection{db: db}
	{{end}}{{end}}
	return db, nil
}

//Close closes the database
func (db *DB) Close() error {
	return db.db.Close()
}

//Iter implemenst basic iterator functions
type Iter struct {
	it iterator.Iterator
}

//ID returns id of current object
func (it *Iter) ID() int64 {
	key := it.it.Key()
	return lexLoadID(key[len(key)-8:])
}

//Next sets the iterator to the next object, or returns false
func (it *Iter) Next() bool {
	return it.it.Next()
}

//Release closes the iterator
func (it *Iter) Release() {
	it.it.Release()
}

{{range $structName, $struct := $gen}}
{{if $struct.Exported}}
//{{$structName}}Collection represents the collection of {{$structName}}s
type {{$structName}}Collection struct {
	db *DB
}

//Iter{{$structName}} iterates trough all {{$structName}} in db
type Iter{{$structName}} struct {
	*Iter
	col *{{$structName}}Collection
}

//Value returns the {{$structName}} on witch is the iterator
func (it *Iter{{$structName}}) Value() (*{{$structName}}, error) {
	data := it.it.Value()
	var obj {{$structName}}
	err := json.Unmarshal(data, &obj)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

//IterIndex{{$structName}} iterates trough an index for {{$structName}} in db
type IterIndex{{$structName}} struct {
	Iter{{$structName}}
}

//Value returns the {{$structName}} on witch is the iterator
func (it *IterIndex{{$structName}}) Value() (*{{$structName}}, error) {
	key := it.it.Key()
	key = append([]byte("{{$structName}}/"), key[len(key)-8:]...)
	data, err := it.col.db.db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	var obj {{$structName}}
	err = json.Unmarshal(data, &obj)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

//Get returns {{$structName}} with specified id or an error
func (col *{{$structName}}Collection) Get(id int64) (*{{$structName}}, error) {
	data, err := col.db.db.Get(append([]byte("{{$structName}}/"), lexDumpID(id)...), nil)
	if err != nil {
		return nil, err
	}
	var obj {{$structName}}
	err = json.Unmarshal(data, &obj)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

//Add inserts new {{$structName}} to the db
func (col *{{$structName}}Collection) Add(obj *{{$structName}}) (int64, error) {
    data, err := json.Marshal(obj)
    if err != nil {
        return 0, fmt.Errorf("json encoding struct {{$structName}} error: %s", err)
    }
    batch := new(leveldb.Batch)
	id := rand.Int63()
    idDump := lexDumpID(id)
    batch.Put(append([]byte("{{$structName}}/"), idDump...), data)
	{{if hasIndex $structName}}
    err = col.addIndex([]byte("${{$structName}}/"), batch, idDump, obj)
    if err != nil {
        return 0, err
    }
	{{end}}
    err = col.db.db.Write(batch, nil)
    if err != nil {
        return 0, err
    }
    return id, nil
}

//Update updates {{$structName}} with specified id
func (col *{{$structName}}Collection) Update(id int64, obj *{{$structName}}) error {
	idDump := lexDumpID(id)
    key := append([]byte("{{$structName}}/"), idDump...)
	{{if hasIndex $structName}}
    oldObj, err := col.Get(id)
    if err != nil {
        return fmt.Errorf("{{$structName}} with id (%d) doesn't exist: %s", id, err)
    }
	{{else}}
	_, err := col.db.db.Get(key, nil)
	if err != nil {
		return nil
	}
	{{end}}
    data, err := json.Marshal(obj)
    if err != nil {
        return fmt.Errorf("json encoding struct {{$structName}} error: %s", err)
    }
    batch := new(leveldb.Batch)
    batch.Put(key, data)
	{{if hasIndex $structName}}
    err = col.removeIndex([]byte("${{$structName}}/"), batch, idDump, oldObj)
    if err != nil {
        return err
    }
    err = col.addIndex([]byte("${{$structName}}/"), batch, idDump, obj)
    if err != nil {
        return err
    }
	{{end}}
    err = col.db.db.Write(batch, nil)
    if err != nil {
        return err
    }
    return nil
}

//Delete removes {{$structName}} from the db with specified id
func (col *{{$structName}}Collection) Delete(id int64) error {
	idDump := lexDumpID(id)
    key := append([]byte("{{$structName}}/"), idDump...)
	{{if hasIndex $structName}}
    oldObj, err := col.Get(id)
    if err != nil {
        return nil
    }
	{{else}}
	_, err := col.db.db.Get(key, nil)
	if err != nil {
		return nil
	}
	{{end}}
    batch := new(leveldb.Batch)
	batch.Delete(key)
	{{if hasIndex $structName}}
    err = col.removeIndex([]byte("${{$structName}}/"), batch, idDump, oldObj)
    if err != nil {
        return fmt.Errorf("Removing {{$structName}} error: %s", err)
    }
	{{end}}
    err = col.db.db.Write(batch, nil)
    if err != nil {
        return err
    }
    return nil
}

//All returns an iterator witch iterates trough all {{$structName}}s
func (col *{{$structName}}Collection) All() *Iter{{$structName}} {
	return &Iter{{$structName}}{
		Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix([]byte("{{$structName}}/")), nil)},
		col: col,
	}
}

{{range $indexName, $subType := (getIndex $structName)}}
//{{$indexName}}Eq iterates trough {{$structName}} {{$indexName}} index with equal values
func (col *{{$structName}}Collection) {{$indexName}}Eq(val {{$subType}}) *IterIndex{{$structName}} {
	valDump := lexDump{{lexType $subType}}(val)
	prefix := append([]byte("${{$structName}}/{{$indexName}}/"), valDump...)
	return &IterIndex{{$structName}}{
		Iter{{$structName}}{
			Iter: &Iter{col.db.db.NewIterator(util.BytesPrefix(prefix), nil)},
			col: col,
		},
	}
}

//{{$indexName}}Range iterates trough {{$structName}} {{$indexName}} index in the specified range
func (col *{{$structName}}Collection) {{$indexName}}Range(start, limit *{{$subType}}) *IterIndex{{$structName}} {
	startDump := []byte("${{$structName}}/{{$indexName}}/")
	if start != nil {
		startDump = append(startDump, lexDump{{lexType $subType}}(*start)...)
	}
	var limitDump []byte
	if limit != nil {
		limitDump = append([]byte("${{$structName}}/{{$indexName}}/"), lexDump{{lexType $subType}}(*limit)...)
	} else {
		limitDump = util.BytesPrefix([]byte("${{$structName}}/{{$indexName}}/")).Limit
	}
	return &IterIndex{{$structName}}{
		Iter{{$structName}}{
			Iter: &Iter{col.db.db.NewIterator(&util.Range{
				Start: startDump,
				Limit: limitDump,
			}, nil)},
			col: col,
		},
	}
}
{{end}}
{{end}}

{{if hasIndex $structName}}
{{if $struct.Exported}}
func (col *{{$structName}}Collection) addIndex(prefix []byte, batch *leveldb.Batch, idDump []byte , obj *{{$structName}}) (err error) {
{{else}}
func (db *DB) add{{$structName}}Index(prefix []byte, batch *leveldb.Batch, idDump []byte, obj *{{$structName}}) (err error) {
{{end}}
	if obj == nil {
		return nil
	}
	var offset int
	var valDump, key []byte
    {{range $attrName, $attr := $struct.Attrs}}
    {{if isStruct $attr.Type}}
		{{if hasIndex $attr.Type}}
		{{if (contains $attr.Type "[]")}}
			for _, attr := range obj.{{$attrName}} {
				{{if isExported (replace (replace $attr.Type "[]" "") "*" "")}}
				err = {{if $struct.Exported}}col.{{end}}db.{{replace (replace $attr.Type "[]" "") "*" ""}}s.addIndex(prefix, batch, idDump, {{if not (contains $attr.Type "*")}}&{{end}}attr)
				{{else}}
				err = {{if $struct.Exported}}col.{{end}}db.add{{replace (replace $attr.Type "[]" "") "*" ""}}Index(prefix, batch, idDump, {{if not (contains $attr.Type "*")}}&{{end}}attr)
				{{end}}
				if err != nil {
					return err
				}
			}
    	{{else}}
			err = db.{{replace $attr.Type "*" ""}}s.addIndex(prefix, batch, idDump, obj.{{$attrName}})
			if err != nil {
				return err
			}
		{{end}}
		{{end}}
	{{else}}
	    {{if ne $attr.Index ""}}
			{{if (contains $attr.Type "[]") and (ne $attr.Index "[]rune") and (ne $attr.Index "[]byte")}}
				for _, attr := range obj.{{$attrName}} {
					valDump = lexDump{{lexType (replace $attr.Type "[]" "")}}(attr)
					key = make([]byte, len(prefix)+len(valDump)+{{add (len $attr.Index) 10}})
					copy(key, prefix)
					offset = len(prefix)
					copy(key[offset:],[]byte("{{$attr.Index}}/"))
					offset += {{add (len $attr.Index) 1}}
					copy(key[offset:], valDump)
					offset += len(valDump)
					key[offset] = byte('/')
					copy(key[offset+1:], idDump)
					batch.Put(key, nil)
				}
			{{else}}
				valDump = lexDump{{lexType (replace $attr.Type "[]" "")}}(obj.{{$attrName}})
				key = make([]byte, len(prefix)+len(valDump)+{{add (len $attr.Index) 10}})
				copy(key, prefix)
				offset = len(prefix)
				copy(key[offset:],[]byte("{{$attr.Index}}/"))
				offset += {{add (len $attr.Index) 1}}
				copy(key[offset:], valDump)
				offset += len(valDump)
				key[offset] = byte('/')
				copy(key[offset+1:], idDump)
				batch.Put(key, nil)
			{{end}}
		{{end}}
    {{end}}
    {{end}}
    return nil
}

{{if $struct.Exported}}
func (col *{{$structName}}Collection) removeIndex(prefix []byte, batch *leveldb.Batch, idDump []byte, obj *{{$structName}}) (err error) {
{{else}}
func (db *DB) remove{{$structName}}Index(prefix []byte, batch *leveldb.Batch, idDump []byte, obj *{{$structName}}) (err error) {
{{end}}
	if obj == nil {
		return nil
	}
	var offset int
	var valDump, key []byte
    {{range $attrName, $attr := $struct.Attrs}}
    {{if isStruct $attr.Type}}
		{{if hasIndex $attr.Type}}
		{{if (contains $attr.Type "[]")}}
			for _, attr := range obj.{{$attrName}} {
				{{if isExported (replace (replace $attr.Type "[]" "") "*" "")}}
				err = {{if $struct.Exported}}col.{{end}}db.{{replace (replace $attr.Type "[]" "") "*" ""}}s.removeIndex(prefix, batch, idDump, {{if not (contains $attr.Type "*")}}&{{end}}attr)
				{{else}}
				err = {{if $struct.Exported}}col.{{end}}db.remove{{replace (replace $attr.Type "[]" "") "*" ""}}Index(prefix, batch, idDump, {{if not (contains $attr.Type "*")}}&{{end}}attr)
				{{end}}
				if err != nil {
					return err
				}
			}
    	{{else}}
			err = db.{{replace $attr.Type "*" ""}}s.addIndex(prefix, batch, idDump, obj.{{$attrName}})
			if err != nil {
				return err
			}
		{{end}}
		{{end}}
	{{else}}
	    {{if ne $attr.Index ""}}
			{{if (contains $attr.Type "[]") and (ne $attr.Index "[]rune") and (ne $attr.Index "[]byte")}}
				for _, attr := range obj.{{$attrName}} {
					valDump = lexDump{{lexType (replace $attr.Type "[]" "")}}(attr)
					key = make([]byte, len(prefix)+len(valDump)+{{add (len $attr.Index) 10}})
					copy(key, prefix)
					offset = len(prefix)
					copy(key[offset:],[]byte("{{$attr.Index}}/"))
					offset += {{add (len $attr.Index) 1}}
					copy(key[offset:], valDump)
					offset += len(valDump)
					key[offset] = byte('/')
					copy(key[offset+1:], idDump)
					batch.Delete(key)
				}
			{{else}}
				valDump = lexDump{{lexType (replace $attr.Type "[]" "")}}(obj.{{$attrName}})
				key = make([]byte, len(prefix)+len(valDump)+{{add (len $attr.Index) 10}})
				copy(key, prefix)
				offset = len(prefix)
				copy(key[offset:],[]byte("{{$attr.Index}}/"))
				offset += {{add (len $attr.Index) 1}}
				copy(key[offset:], valDump)
				offset += len(valDump)
				key[offset] = byte('/')
				copy(key[offset+1:], idDump)
				batch.Delete(key)
			{{end}}
		{{end}}
    {{end}}
    {{end}}
    return nil
}
{{end}}
{{end}}
`

//DBGenerator represents the structs in file
type DBGenerator struct {
	PackageName string
	Structs     map[string]*Struct
}

//Parse parses the given file and returns the DBGenerator
func Parse(filename string) (*DBGenerator, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("Error in parsing file %s: %s", filename, err)
	}
	gen := &DBGenerator{Structs: make(map[string]*Struct), PackageName: f.Name.Name}
	ast.Inspect(f, func(node ast.Node) bool {
		typ, ok := node.(*ast.GenDecl)
		if !ok || typ.Tok != token.TYPE {
			return true
		}
		for _, spec := range typ.Specs {
			structSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			structType, ok := structSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			parseStructType(gen, structSpec, structType, fset)
		}
		return true
	})
	return gen, nil
}

func parseStructType(gen *DBGenerator, structSpec *ast.TypeSpec, structType *ast.StructType, fset *token.FileSet) {
	s := &Struct{
		Name:     structSpec.Name.Name,
		Exported: ast.IsExported(structSpec.Name.Name),
		Attrs:    make(map[string]*Attr),
	}
	gen.Structs[structSpec.Name.Name] = s
	for _, field := range structType.Fields.List {
		for _, name := range field.Names {
			attr := new(Attr)
			if field.Tag != nil && field.Tag.Kind == token.STRING {
				matches := findIndex.FindStringSubmatch(field.Tag.Value)
				if len(matches) > 1 {
					attr.Index = matches[len(matches)-1]
				}
			}
			switch typ := field.Type.(type) {
			case *ast.Ident:
				attr.Type = typ.Name
			case *ast.StarExpr:
				switch starType := typ.X.(type) {
				case *ast.Ident:
					attr.Type = "*" + starType.Name
				case *ast.SelectorExpr:
					switch selectorIdent := starType.X.(type) {
					case *ast.Ident:
						attr.Type = fmt.Sprintf("*%s.%s", selectorIdent.Name, starType.Sel.Name)
					default:
						fmt.Printf("Error: Unknown array type %s at line %d\n", selectorIdent, fset.File(selectorIdent.Pos()).Line(selectorIdent.Pos()))
						os.Exit(1)
					}
				default:
					fmt.Printf("Error: Unknown pointer type %s at line %d\n", typ.X, fset.File(typ.X.Pos()).Line(typ.X.Pos()))
					os.Exit(1)
				}
			case *ast.ArrayType:
				switch expr := typ.Elt.(type) {
				case *ast.Ident:
					attr.Type = "[]" + expr.Name
				case *ast.StarExpr:
					switch starIdent := expr.X.(type) {
					case *ast.Ident:
						attr.Type = "[]*" + starIdent.Name
					default:
						fmt.Printf("Error: Unknown pointer type %s at line %d\n", starIdent, fset.File(starIdent.Pos()).Line(starIdent.Pos()))
						os.Exit(1)
					}
				default:
					fmt.Printf("Error: Unknown array type %s at line %d\n", expr, fset.File(expr.Pos()).Line(expr.Pos()))
					os.Exit(1)
				}
			case *ast.SelectorExpr:
				switch selectorIdent := typ.X.(type) {
				case *ast.Ident:
					attr.Type = fmt.Sprintf("%s.%s", selectorIdent.Name, typ.Sel.Name)
				default:
					fmt.Printf("Error: Unknown array type %s at line %d\n", selectorIdent, fset.File(selectorIdent.Pos()).Line(selectorIdent.Pos()))
					os.Exit(1)
				}
			default:
				fmt.Printf("Error: Unknown type %#v at line %d\n", typ, fset.File(typ.Pos()).Line(typ.Pos()))
				os.Exit(1)
			}
			if attr.Index != "" && !attr.IsIndexable() {
				fmt.Printf("Error: Type %s is not indexable at line %d\n", attr.Type, fset.File(field.Pos()).Line(field.Pos()))
				os.Exit(1)
			}
			s.Attrs[name.Name] = attr
		}
	}
}

func (gen *DBGenerator) String() string {
	var buf bytes.Buffer
	buf.WriteString("Mímir {\n")
	for structName, s := range gen.Structs {
		buf.WriteString(fmt.Sprintf("\t%s: {\n\t\tExported: %t\n", structName, s.Exported))
		for attrName, attr := range s.Attrs {
			buf.WriteString(fmt.Sprintf("\t\t%s %s", attrName, attr.Type))
			if attr.Index != "" {
				buf.WriteString(fmt.Sprintf(" index:%s", attr.Index))
			}
			buf.WriteRune('\n')
		}
		buf.WriteString("\t},\n")
	}
	buf.WriteString("}")
	return buf.String()
}

func (gen *DBGenerator) hasIndex(structName string) bool {
	s, ok := gen.Structs[strings.Replace(strings.Replace(structName, "[]", "", -1), "*", "", -1)]
	if !ok {
		return false
	}
	for _, attr := range s.Attrs {
		if gen.isStruct(attr.Type) {
			if gen.hasIndex(attr.Type) {
				return true
			}
		} else if attr.Index != "" {
			return true
		}
	}
	return false
}

func (gen *DBGenerator) getIndex(structName string) map[string]string {
	s, ok := gen.Structs[strings.Replace(strings.Replace(structName, "[]", "", -1), "*", "", -1)]
	if !ok {
		return nil
	}
	ret := make(map[string]string)
	for _, attr := range s.Attrs {
		if gen.isStruct(attr.Type) {
			for k, v := range gen.getIndex(attr.Type) {
				ret[k] = v
			}
		} else if attr.Index != "" {
			ret[attr.Index] = attr.Type
		}
	}
	return ret
}

func (gen *DBGenerator) isStruct(structName string) bool {
	_, ok := gen.Structs[strings.Replace(strings.Replace(structName, "[]", "", -1), "*", "", -1)]
	return ok
}

//Generate generates the db sourcecode
func (gen DBGenerator) Generate(w io.Writer) error {
	funcMap := template.FuncMap{
		"isStruct":   gen.isStruct,
		"replace":    func(o, old, new string) string { return strings.Replace(o, old, new, -1) },
		"contains":   strings.Contains,
		"hasIndex":   gen.hasIndex,
		"getIndex":   gen.getIndex,
		"isExported": ast.IsExported,
		"lexType":    lexType,
		"add":        func(a, b int) int { return a + b },
	}
	buf := bytes.NewBuffer(nil)
	temp, err := template.New("").Funcs(funcMap).Parse(DBTEMPLATE)
	if err != nil {
		return err
	}
	err = temp.Execute(buf, gen)
	if err != nil {
		return fmt.Errorf("Template execution error: %v", err)
	}
	src, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("format source error: $%v", err)
	}
	w.Write(src)
	return nil
}

//Struct is the struct from whitch the collections are generated
type Struct struct {
	Name     string
	Exported bool
	Attrs    map[string]*Attr
}

//Attr is the attribute of a struct, its type and if the attribute will be indexed
type Attr struct {
	Type  string
	Index string
}

//IsIndexable returns true if the attribute type has suporting lexDump
func (attr *Attr) IsIndexable() bool {
	_, ok := INDEXABLE[attr.Type]
	return ok
}
