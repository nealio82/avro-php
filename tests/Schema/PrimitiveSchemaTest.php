<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use PHPUnit\Framework\TestCase;

class PrimitiveSchemaTest extends TestCase
{
    /**
     * @dataProvider schemaExamplesProvider
     */
    public function testParse(string $schemaString, bool $isValid, ?string $expectedValue): void
    {
        try {
            $schema = Schema::parse($schemaString);

            $this->assertTrue($isValid, sprintf("schema_string: %s\n", $schemaString));
            $this->assertEquals($expectedValue, $schema->toAvro());
        } catch (SchemaParseException $e) {
            $this->assertFalse(
                $isValid,
                sprintf("schema_string: %s\n%s", $schemaString, $e->getMessage())
            );
        }
    }

    public function schemaExamplesProvider(): iterable
    {
        // schemaString isValid expectedValue
        yield ['"null"', true, 'null'];
        yield ['{"type": "null"}', true, 'null'];
        yield ['"boolean"', true, 'boolean'];
        yield ['{"type": "boolean"}', true, 'boolean'];
        yield ['"int"', true, 'int'];
        yield ['{"type": "int"}', true, 'int'];
        yield ['"long"', true, 'long'];
        yield ['{"type": "long"}', true, 'long'];
        yield ['"float"', true, 'float'];
        yield ['{"type": "float"}', true, 'float'];
        yield ['"double"', true, 'double'];
        yield ['{"type": "double"}', true, 'double'];
        yield ['"bytes"', true, 'bytes'];
        yield ['{"type": "bytes"}', true, 'bytes'];
        yield ['"string"', true, 'string'];
        yield ['{"type": "string"}', true, 'string'];
        yield ['"True"', false, null];
        yield ['{"no_type": "test"}', false, null];
        yield ['{"type": "panther"}', false, null];
    }
}
