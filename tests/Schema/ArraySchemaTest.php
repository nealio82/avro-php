<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use PHPUnit\Framework\TestCase;

class ArraySchemaTest extends TestCase
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
        yield ['{"type": "array", "items": "long"}', true];
        yield ['{"type": "array", "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}', true];
    }
}
