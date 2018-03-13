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
        yield ['{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', true];
        yield ['{"type": "enum", "name": "Status", "symbols": "Normal Caution Critical"}', false];
        yield ['{"type": "enum", "name": [ 0, 1, 1, 2, 3, 5, 8 ], "symbols": ["Golden", "Mean"]}', false];
        yield ['{"type": "enum", "symbols" : ["I", "will", "fail", "no", "name"]}', false];
        yield ['{"type": "enum", "name": "Test" "symbols" : ["AA", "AA"]}', false];
        yield ['{"type":"enum","name":"Test","symbols":["AA", 16]}', false];
        yield ['{"type": "enum", "name": "blood_types", "doc": "AB is freaky.", "symbols" : ["A", "AB", "B", "O"]}', true];
        yield ['{"type": "enum", "name": "blood-types", "doc": 16, "symbols" : ["A", "AB", "B", "O"]}', false];
    }
}
