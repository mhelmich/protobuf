package avroschema

import (
	"fmt"
	"os"

	"github.com/gogo/protobuf/gogoproto"
	"github.com/gogo/protobuf/protoc-gen-gogo/generator"
)

type avroschema struct {
	*generator.Generator
	generator.PluginImports
	localName string
}

func newAvroSchema() *avroschema {
	return &avroschema{}
}

func (p *avroschema) Name() string {
	return "avroschema"
}

func (p *avroschema) Init(g *generator.Generator) {
	p.Generator = g
}

func (p *avroschema) Generate(file *generator.FileDescriptor) {
	p.PluginImports = generator.NewPluginImports(p.Generator)
	p.localName = generator.FileName(file)
	fmt.Fprintf(os.Stderr, "ERROR: %s ======================================================\n", p.localName)
	for _, message := range file.Messages() {
		if gogoproto.HasAvroSchema(file.FileDescriptorProto, message.DescriptorProto) {
			p.processMessage(message)
		}
	}
}

func (p *avroschema) processMessage(message *generator.Descriptor) {
	ccTypeName := generator.CamelCaseSlice(message.TypeName())
	p.P(`func (this *`, ccTypeName, `) AvroSchema() string {`)
	p.In()
	p.P(`if this == nil {`)
	p.In()
	p.P(`return "nil"`)
	p.Out()
	p.P(`}`)
	p.P(`return ""`)
	p.Out()
	p.P(`}`)
}

func init() {
	generator.RegisterPlugin(newAvroSchema())
}
