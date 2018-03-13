<?php

namespace Avro\Datum;

use Avro\Debug\Debug;
use Avro\IO\StringIO;
use Avro\Schema\Schema;
use PHPUnit\Framework\TestCase;

class DatumIOTest extends TestCase
{
    /**
     * @dataProvider dataProvider
     *
     * @param mixed $datum
     */
    public function testDatumRoundTrip(string $schemaJson, $datum, string $binary): void
    {
        $schema = Schema::parse($schemaJson);
        $written = new StringIO();
        $encoder = new IOBinaryEncoder($written);
        $writer = new IODatumWriter($schema);

        $writer->write($datum, $encoder);
        $output = (string) $written;
        $this->assertEquals(
            $binary,
            $output,
            sprintf(
                "expected: %s\n  actual: %s",
                Debug::ascii_string($binary, 'hex'),
                Debug::ascii_string($output, 'hex')
            )
        );

        $read = new StringIO($binary);
        $decoder = new IOBinaryDecoder($read);
        $reader = new IODatumReader($schema);
        $readDatum = $reader->read($decoder);
        $this->assertEquals($datum, $readDatum);
    }

    public function dataProvider(): iterable
    {
        // schemaJson datum binary
        yield ['"null"', null, ''];

        yield ['"boolean"', true, "\001"];
        yield ['"boolean"', false, "\000"];

        yield ['"int"', -2147483648, "\xFF\xFF\xFF\xFF\x0F"];
        yield ['"int"', -1, "\001"];
        yield ['"int"', 0, "\000"];
        yield ['"int"', 1, "\002"];
        yield ['"int"', 2147483647, "\xFE\xFF\xFF\xFF\x0F"];

//        yield ['"long"', -9223372036854775808, "\001"];
        yield ['"long"', -1, "\001"];
        yield ['"long"', 0, "\000"];
        yield ['"long"', 1, "\002"];
//        yield ['"long"', 9223372036854775807, "\002"];

        yield ['"float"', (float) -10.0, "\000\000 \301"];
        yield ['"float"', (float) -1.0, "\000\000\200\277"];
        yield ['"float"', 0.0, "\000\000\000\000"];
        yield ['"float"', 2.0, "\000\000\000@"];
        yield ['"float"', 9.0, "\000\000\020A"];

        yield ['"double"', (float) -10.0, "\000\000\000\000\000\000$\300"];
        yield ['"double"', (float) -1.0, "\000\000\000\000\000\000\360\277"];
        yield ['"double"', 0.0, "\000\000\000\000\000\000\000\000"];
        yield ['"double"', 2.0, "\000\000\000\000\000\000\000@"];
        yield ['"double"', 9.0, "\000\000\000\000\000\000\"@"];

        yield ['"string"', 'foo', "\x06foo"];
        yield ['"bytes"', "\x01\x02\x03", "\x06\x01\x02\x03"];

        yield ['{"type":"array","items":"int"}', [1, 2, 3], "\x06\x02\x04\x06\x00"];
        yield ['{"type":"map","values":"int"}', ['foo' => 1, 'bar' => 2, 'baz' => 3], "\x06\x06foo\x02\x06bar\x04\x06baz\x06\x00"];
        yield ['["null", "int"]', 1, "\x02\x02"];
        yield ['{"name":"fix","type":"fixed","size":3}', "\xAA\xBB\xCC", "\xAA\xBB\xCC"];
        yield ['{"name":"enm","type":"enum","symbols":["A","B","C"]}', 'B', "\x02"];
        yield ['{"name":"rec","type":"record","fields":[{"name":"a","type":"int"},{"name":"b","type":"boolean"}]}', ['a' => 1, 'b' => false], "\x02\x00"];
    }

    /**
     * @dataProvider defaultValueProvider
     *
     * @param mixed $defaultValue
     */
    public function testFieldDefaultValue(string $fieldSchemaJson, string $defaultJson, $defaultValue): void
    {
        $writersSchema = Schema::parse('{"name":"foo","type":"record","fields":[]}');

        $readersSchemaJson = sprintf(
            '{"name":"foo","type":"record","fields":[{"name":"f","type":%s,"default":%s}]}',
            $fieldSchemaJson,
            $defaultJson
        );
        $readersSchema = Schema::parse($readersSchemaJson);

        $reader = new IODatumReader($writersSchema, $readersSchema);
        $record = $reader->read(new IOBinaryDecoder(new StringIO()));
        if (array_key_exists('f', $record)) {
            $this->assertEquals($defaultValue, $record['f']);
        } else {
            $this->assertTrue(
                false,
                sprintf(
                    'expected field record[f]: %s',
                    print_r($record, true)
                )
            );
        }
    }

    public function defaultValueProvider(): iterable
    {
        yield ['"null"', 'null', null];
        yield ['"boolean"', 'true', true];
        yield ['"int"', '1', 1];
        yield ['"long"', '2000', 2000];
        yield ['"float"', '1.1', 1.1];
        yield ['"double"', '200.2', 200.2];
        yield ['"string"', '"quux"', 'quux'];
        yield ['"bytes"', '"\u00FF"', "\xC3\xBF"];
        yield ['{"type":"array","items":"int"}', '[5,4,3,2]', [5, 4, 3, 2]];
        yield ['{"type":"map","values":"int"}', '{"a":9}', ['a' => 9]];
        yield ['["int","string"]', '8', 8];
        yield ['{"name":"x","type":"enum","symbols":["A","V"]}', '"A"', 'A'];
        yield ['{"name":"x","type":"fixed","size":4}', '"\u00ff"', "\xC3\xBF"];
        yield ['{"name":"x","type":"record","fields":[{"name":"label","type":"int"}]}', '{"label":7}', ['label' => 7]];
    }
}
