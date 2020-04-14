package avroschema

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/gogoproto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/gogo/protobuf/protoc-gen-gogo/generator"
)

func init() {
	generator.RegisterPlugin(newAvroSchema())
}

var errTypeNotFound = errors.New("type not found")

type avroField interface {
	schema() string
}

type envelope struct {
	name      string
	namespace string
	typ       string
	fields    []avroField
}

func (e *envelope) schema() string {
	var fields []string
	for _, field := range e.fields {
		fields = append(fields, field.schema())
	}

	return fmt.Sprintf("{\"name\": \"%s\", \"type\": \"record\", \"namespace\": \"%s\", \"fields\": [ %s ]}", e.name, e.namespace, strings.Join(fields, ", "))
}

type simpleField struct {
	name string
	typ  string
}

func (f *simpleField) schema() string {
	return fmt.Sprintf("{\"name\": \"%s\", \"type\": \"%s\"}", f.name, f.typ)
}

type complexField struct {
	fieldName string
	typeName  string
	fields    []avroField
}

func (f *complexField) schema() string {
	var fields []string
	for _, field := range f.fields {
		fields = append(fields, field.schema())
	}

	return fmt.Sprintf("{\"name\": \"%s\", \"type\": { \"name\": \"%s\", \"type\": \"record\", \"fields\": [ %s ] }}", f.fieldName, f.typeName, strings.Join(fields, ", "))
}

type arrayField struct {
	name string
	typ  string
}

func (f *arrayField) schema() string {
	return fmt.Sprintf("{\"name\": \"%s\", \"type\": {\"type\": \"array\", \"items\": \"%s\"}}", f.name, f.typ)
}

type enumField struct {
	name     string
	typeName string
	symbols  []string
}

func (f *enumField) schema() string {
	var quoted []string
	for _, str := range f.symbols {
		quoted = append(quoted, strconv.Quote(str))
	}
	return fmt.Sprintf("{\"name\": \"%s\", \"type\": {\"type\": \"enum\", \"name\": \"%s\", \"symbols\": [ %s ]}}", f.name, f.typeName, strings.Join(quoted, ", "))
}

type avroschema struct {
	*generator.Generator
	generator.PluginImports
	fileName string
	seen     map[string]avroField
}

func newAvroSchema() *avroschema {
	return &avroschema{}
}

func (p *avroschema) _print(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
}

func (p *avroschema) Name() string {
	return "avroschema"
}

func (p *avroschema) Init(g *generator.Generator) {
	p.Generator = g
	p.seen = make(map[string]avroField)
}

func (p *avroschema) Generate(file *generator.FileDescriptor) {
	p.PluginImports = generator.NewPluginImports(p.Generator)
	p.fileName = generator.FileName(file)
	for _, message := range file.Messages() {
		if gogoproto.HasAvroSchema(file.FileDescriptorProto, message.DescriptorProto) {
			p.createMessageStub(message)
		}
	}
}

func (p *avroschema) createMessageStub(message *generator.Descriptor) {
	ccTypeName := message.GetName()
	schema := p.processMessage(message)
	p.P(`func AvroSchemaFor`, ccTypeName, `() string {`)
	p.In()
	p.P(fmt.Sprintf("return %s", strconv.Quote(schema.schema())))
	p.Out()
	p.P(`}`)
	p.WriteByte('\n')
}

func (p *avroschema) processMessage(message *generator.Descriptor) *envelope {
	e := &envelope{
		typ:       "record",
		namespace: p.fileName,
		name:      generator.CamelCaseSlice(message.TypeName()),
		fields:    make([]avroField, 0),
	}
	p.seen[generator.CamelCaseSlice(message.TypeName())] = e

	for _, field := range message.Field {
		fs, err := p.processField(message, field)
		if err != nil {
			panic(fmt.Sprintf("Rats! %s", err.Error()))
		}
		e.fields = append(e.fields, fs)
	}

	return e
}

func (p *avroschema) processField(message *generator.Descriptor, field *descriptor.FieldDescriptorProto) (avroField, error) {
	if field.IsRepeated() {
		return p.getArrayField(message, field)
	}

	switch *field.Type {
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE,
		descriptor.FieldDescriptorProto_TYPE_FLOAT,
		descriptor.FieldDescriptorProto_TYPE_INT64,
		descriptor.FieldDescriptorProto_TYPE_UINT64,
		descriptor.FieldDescriptorProto_TYPE_INT32,
		descriptor.FieldDescriptorProto_TYPE_UINT32,
		descriptor.FieldDescriptorProto_TYPE_FIXED64,
		descriptor.FieldDescriptorProto_TYPE_FIXED32,
		descriptor.FieldDescriptorProto_TYPE_BOOL,
		descriptor.FieldDescriptorProto_TYPE_STRING,
		descriptor.FieldDescriptorProto_TYPE_SFIXED32,
		descriptor.FieldDescriptorProto_TYPE_SFIXED64,
		descriptor.FieldDescriptorProto_TYPE_SINT32,
		descriptor.FieldDescriptorProto_TYPE_SINT64,
		descriptor.FieldDescriptorProto_TYPE_BYTES:
		return p.getSimpleAvroField(message, field)

	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		return p.getComplexField(message, field)
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		return p.getEnumField(message, field)

	case descriptor.FieldDescriptorProto_TYPE_GROUP:
		// do nothing - groups are deprecated
		return nil, nil

	default:
		p.Fail("unknown type for ", field.GetName())
		return nil, errTypeNotFound
	}
}

func (p *avroschema) getSimpleAvroField(message *generator.Descriptor, field *descriptor.FieldDescriptorProto) (*simpleField, error) {
	return &simpleField{
		name: p.GetFieldName(message, field),
		typ:  p.getAvroTypeForProtobufType(field.Type),
	}, nil
}

func (p *avroschema) getArrayField(message *generator.Descriptor, field *descriptor.FieldDescriptorProto) (*arrayField, error) {
	return &arrayField{
		name: p.GetFieldName(message, field),
		typ:  p.getAvroTypeForProtobufType(field.Type),
	}, nil
}

func (p *avroschema) getComplexField(message *generator.Descriptor, field *descriptor.FieldDescriptorProto) (*complexField, error) {
	splits := strings.Split(generator.CamelCase(field.GetTypeName()), ".")
	typeName := splits[len(splits)-1]
	cached, ok := p.seen[typeName]
	if ok {
		// convert envelope into complex field
		envelope, ok := cached.(*envelope)
		if ok {
			return &complexField{
				fieldName: generator.CamelCase(field.GetName()),
				typeName:  typeName,
				fields:    envelope.fields,
			}, nil
		}

		cf, ok := cached.(*complexField)
		if ok {
			return cf, nil
		}

		panic("Rats! Cache contains type I didn't expect")
	}

	f := &complexField{
		fieldName: generator.CamelCase(*field.Name),
		typeName:  typeName,
		fields:    make([]avroField, 0),
	}

	p.seen[generator.CamelCase(field.GetName())] = f
	return f, nil
}

func (p *avroschema) getEnumField(message *generator.Descriptor, field *descriptor.FieldDescriptorProto) (*enumField, error) {
	enum := p.ObjectNamed(field.GetTypeName()).(*generator.EnumDescriptor)
	var symbols []string
	for _, v := range enum.GetValue() {
		symbols = append(symbols, v.GetName())
	}

	splits := strings.Split(field.GetTypeName(), ".")
	return &enumField{
		name:     field.GetName(),
		typeName: splits[len(splits)-1],
		symbols:  symbols,
	}, nil
}

func (p *avroschema) getAvroTypeForProtobufType(t *descriptor.FieldDescriptorProto_Type) string {
	switch *t {
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return "double"
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return "float"
	case descriptor.FieldDescriptorProto_TYPE_INT64:
		return "long"
	case descriptor.FieldDescriptorProto_TYPE_UINT64:
		return "long"
	case descriptor.FieldDescriptorProto_TYPE_INT32:
		return "int"
	case descriptor.FieldDescriptorProto_TYPE_UINT32:
		return "int"
	case descriptor.FieldDescriptorProto_TYPE_FIXED64:
		return "long"
	case descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return "int"
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return "boolean"
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return "string"
	case descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return "int"
	case descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return "long"
	case descriptor.FieldDescriptorProto_TYPE_SINT32:
		return "int"
	case descriptor.FieldDescriptorProto_TYPE_SINT64:
		return "long"
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return "bytes"
	default:
		return "nil"
	}
}
