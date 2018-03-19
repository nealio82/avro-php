<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use PHPUnit\Framework\TestCase;

class RecordSchemaTest extends TestCase
{
    /**
     * @dataProvider schemaExamplesProvider
     */
    public function testParse(string $schemaString, bool $isValid, ?string $expectedValue): void
    {
        try {
            $schema = Schema::parse($schemaString);

            $this->assertTrue($isValid, sprintf("schema_string: %s\n", $schemaString));
            $this->assertEquals(json_decode($expectedValue ?: $schemaString, true), $schema->toAvro());
        } catch (SchemaParseException $e) {
            $this->assertFalse(
                $isValid,
                sprintf("schema_string: %s\n%s", $schemaString, $e->getMessage())
            );
        }
    }

    public function schemaExamplesProvider(): iterable
    {
        // schemaString isValid
//        yield ['{"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}', true, null];
//        yield ['{"type": "error", "name": "Test", "fields": [{"name": "f", "type": "long"}]}', true, null];
//        yield ['{"type": "record", "name": "Node", "fields": [{"name": "label", "type": "string"}, {"name": "children", "type": {"type": "array", "items": "Node"}}]}', true, null];
//        yield ['{"type": "record", "name": "ListLink", "fields": [{"name": "car", "type": "int"}, {"name": "cdr", "type": "ListLink"}]}', true, null];
//        yield ['{"type": "record", "name": "Lisp", "fields": [{"name": "value", "type": ["null", "string"]}]}', true, null];
//        yield ['{"type": "record", "name": "Lisp", "fields": [{"name": "value", "type": ["null", "string", {"type": "record", "name": "Cons", "fields": [{"name": "car", "type": "string"}, {"name": "cdr", "type": "string"}]}]}]}', true, null];
//        yield ['{"type": "record", "name": "Lisp", "fields": [{"name": "value", "type": ["null", "string", {"type": "record", "name": "Cons", "fields": [{"name": "car", "type": "Lisp"}, {"name": "cdr", "type": "Lisp"}]}]}]}', true, null];
        yield ['{"type": "record", "name": "HandshakeRequest", "namespace": "org.apache.avro.ipc", "fields": [{"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}}, {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}]}', true, null];
//        yield ['{"type": "record", "name": "HandshakeRequest", "namespace": "org.apache.avro.ipc", "fields": [{"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}}, {"name": "clientProtocol", "type": ["null", "string"]}, {"name": "serverHash", "type": "MD5"}, {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}]}', true, null];
//        yield ['{"type": "record", "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc", "fields": [{"name": "match", "type": {"type": "enum", "name": "HandshakeMatch", "symbols": ["BOTH", "CLIENT", "NONE"]}}, {"name": "serverProtocol", "type": ["null", "string"]}, {"name": "serverHash", "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}]}, {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}]}', true, null];
//        yield ['{"type": "record", "namespace": "org.apache.avro", "name": "Interop", "fields": [{"type": {"fields": [{"type": {"items": "org.apache.avro.Node", "type": "array"}, "name": "children"}], "type": "record", "name": "Node"}, "name": "recordField"}]}', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"children","type":{"type":"array","items":"Node"}}]}}]}'];
//        yield ['{"type": "record", "namespace": "org.apache.avro", "name": "Interop", "fields": [{"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"}, "name": "enumField"}, {"type": {"fields": [{"type": "string", "name": "label"}, {"type": {"items": "org.apache.avro.Node", "type": "array"}, "name": "children"}], "type": "record", "name": "Node"}, "name": "recordField"}]}', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}'];
//        yield ['{"type": "record", "name": "Interop", "namespace": "org.apache.avro", "fields": [{"name": "intField", "type": "int"}, {"name": "longField", "type": "long"}, {"name": "stringField", "type": "string"}, {"name": "boolField", "type": "boolean"}, {"name": "floatField", "type": "float"}, {"name": "doubleField", "type": "double"}, {"name": "bytesField", "type": "bytes"}, {"name": "nullField", "type": "null"}, {"name": "arrayField", "type": {"type": "array", "items": "double"}}, {"name": "mapField", "type": {"type": "map", "values": {"name": "Foo", "type": "record", "fields": [{"name": "label", "type": "string"}]}}}, {"name": "unionField", "type": ["boolean", "double", {"type": "array", "items": "bytes"}]}, {"name": "enumField", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}}, {"name": "fixedField", "type": {"type": "fixed", "name": "MD5", "size": 16}}, {"name": "recordField", "type": {"type": "record", "name": "Node", "fields": [{"name": "label", "type": "string"}, {"name": "children", "type": {"type": "array", "items": "Node"}}]}}]}', true, null];
//        yield ['{"type": "record", "namespace": "org.apache.avro", "name": "Interop", "fields": [{"type": "int", "name": "intField"}, {"type": "long", "name": "longField"}, {"type": "string", "name": "stringField"}, {"type": "boolean", "name": "boolField"}, {"type": "float", "name": "floatField"}, {"type": "double", "name": "doubleField"}, {"type": "bytes", "name": "bytesField"}, {"type": "null", "name": "nullField"}, {"type": {"items": "double", "type": "array"}, "name": "arrayField"}, {"type": {"type": "map", "values": {"fields": [{"type": "string", "name": "label"}], "type": "record", "name": "Foo"}}, "name": "mapField"}, {"type": ["boolean", "double", {"items": "bytes", "type": "array"}], "name": "unionField"}, {"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"}, "name": "enumField"}, {"type": {"type": "fixed", "name": "MD5", "size": 16}, "name": "fixedField"}, {"type": {"fields": [{"type": "string", "name": "label"}, {"type": {"items": "org.apache.avro.Node", "type": "array"}, "name": "children"}], "type": "record", "name": "Node"}, "name": "recordField"}]}', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"stringField","type":"string"},{"name":"boolField","type":"boolean"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"double"}},{"name":"mapField","type":{"type":"map","values":{"type":"record","name":"Foo","fields":[{"name":"label","type":"string"}]}}},{"name":"unionField","type":["boolean","double",{"type":"array","items":"bytes"}]},{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"fixedField","type":{"type":"fixed","name":"MD5","size":16}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}'];
//        yield ['{"type": "record", "name": "ipAddr", "fields": [{"name": "addr", "type": [{"name": "IPv6", "type": "fixed", "size": 16}, {"name": "IPv4", "type": "fixed", "size": 4}]}]}', true, null];
//        yield ['{"type": "record", "name": "Event", "fields": [{"name": "Sponsor"}, {"name": "City", "type": "string"}]}', false, null];
//        yield ['{"type": "record", "fields": "His vision, from the constantly passing bars," "name", "Rainer"}', false, null];
//        yield ['{"type":"record","name":"foo","doc":"doc string", "fields":[{"name":"bar", "type":"int", "order":"ascending", "default":1}]}', true, null];
//        yield ['{"type":"record", "name":"foo", "doc":"doc string", "fields":[{"name":"bar", "type":"int", "order":"bad"}]}', false, null];
    }
}
