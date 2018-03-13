<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use PHPUnit\Framework\TestCase;

class UnionSchemaTest extends TestCase
{
    /**
     * @dataProvider schemaExamplesProvider
     */
    public function testParse(string $schemaString, bool $isValid): void
    {
        try {
            $schema = Schema::parse($schemaString);

            $this->assertTrue($isValid, sprintf("schema_string: %s\n", $schemaString));
            $this->assertEquals(json_decode($schemaString, true), $schema->to_avro());
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
        yield ['["string", "null", "long"]', true];
        yield ['["null", "null"]', false];
        yield ['["long", "long"]', false];
        yield ['[{"type": "array", "items": "long"} {"type": "array", "items": "string"}]', false];
        yield ['["long", {"type": "long"}, "int"]', false];
        yield ['["long", {"type": "array", "items": "long"}, {"type": "map", "values": "long"}, "int"]', true];
        yield ['["long", ["string", "null"], "int"]', false];
        yield ['["null", "boolean", "int", "long", "float", "double", "string", "bytes", {"type": "array", "items":"int"}, {"type": "map", "values":"int"}, {"name": "bar", "type":"record", "fields":[{"name":"label", "type":"string"}]}, {"name": "foo", "type":"fixed", "size":16}, {"name": "baz", "type":"enum", "symbols":["A", "B", "C"]}]', true];
        yield ['[{"name":"subtract", "namespace":"com.example", "type":"record", "fields":[{"name":"minuend", "type":"int"}, {"name":"subtrahend", "type":"int"}]}, {"name": "divide", "namespace":"com.example", "type":"record", "fields":[{"name":"quotient", "type":"int"}, {"name":"dividend", "type":"int"}]}, {"type": "array", "items": "string"}]', true];
    }
}
