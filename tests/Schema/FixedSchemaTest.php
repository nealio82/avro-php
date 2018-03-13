<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use PHPUnit\Framework\TestCase;

class FixedSchemaTest extends TestCase
{
    /**
     * @dataProvider schemaExamplesProvider
     */
    public function testParse(string $schemaString, bool $isValid, ?string $expectedValue): void
    {
        try {
            $schema = Schema::parse($schemaString);

            $this->assertTrue($isValid, sprintf("schema_string: %s\n", $schemaString));
            $this->assertEquals(json_decode($expectedValue ?: $schemaString, true), $schema->to_avro());
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
        yield ['{"type": "fixed", "name": "Test", "size": 1}', true, null];
        yield ['{"type": "fixed", "name": "MyFixed", "namespace": "org.apache.hadoop.avro", "size": 1}', true, null];
        yield ['{"type": "fixed", "name": "Missing size"}', false, null];
        yield ['{"type": "fixed", "size": 314}', false, null];
        yield ['{"type":"fixed","name":"ex","doc":"this should be ignored","size": 314}', true, '{"type":"fixed","name":"ex","size":314}'];
        yield ['{"name": "bar", "namespace": "com.example", "type": "fixed", "size": 32 }', true, '{"type":"fixed","name":"bar","namespace":"com.example","size":32}'];
        yield ['{"name": "com.example.bar", "type": "fixed", "size": 32 }', true, '{"type":"fixed","name":"bar","namespace":"com.example","size":32}'];
        yield ['{"type":"fixed","name":"_x.bar","size":4}', true, '{"type":"fixed","name":"bar","namespace":"_x","size":4}'];
        yield ['{"type":"fixed","name":"baz._x","size":4}', true, '{"type":"fixed","name":"_x","namespace":"baz","size":4}'];
        yield ['{"type":"fixed","name":"baz.3x","size":4}', false, null];
    }
}
