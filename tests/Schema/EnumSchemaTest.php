<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use PHPUnit\Framework\TestCase;

class EnumSchemaTest extends TestCase
{
    /**
     * @dataProvider schemaExamplesProvider
     */
    public function testParse(string $schemaString, bool $isValid): void
    {
        try {
            $schema = Schema::parse($schemaString);

            $this->assertTrue($isValid, sprintf("schema_string: %s\n", $schemaString));
            $this->assertEquals(json_decode($schemaString, true), $schema->toAvro());
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
        yield ['{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', true];
        yield ['{"type": "enum", "name": "Test", "symbols": ["", "B"]}', false];
        yield ['{"type": "enum", "name": "Test", "symbols": ["A", 1]}', false];
        yield ['{"type": "enum", "name": "Test", "symbols": [null, "B"]}', false];
        yield ['{"type": "enum", "name": "blood_types", "doc": "AB is freaky.", "symbols" : ["A", "AB", "B", "O"]}', true];
        yield ['{"type": "enum", "name": "Test" "symbols" : ["AA", "AA"]}', false];
        yield ['{"type":"enum","name":"Test","symbols":["AA", 16]}', false];
        yield ['{"type": "enum", "name": "blood-types", "doc": 16, "symbols" : ["A", "AB", "B", "O"]}', false];
    }
}
