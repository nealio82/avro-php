<?php

namespace Avro\Protocol;

use Avro\Exception\SchemaParseException;
use PHPUnit\Framework\TestCase;

class ProtocolTest extends TestCase
{
    /**
     * @dataProvider protocolProvider
     */
    public function testParsing(string $filename, bool $isParseable): void
    {
        try {
            $protocol = Protocol::parse($this->loadDataFromFile($filename));

            $this->assertInstanceOf(Protocol::class, $protocol);
        } catch (SchemaParseException $x) {
            // Exception ok if we expected this protocol spec to be unparseable.
            $this->assertFalse($isParseable);
        }
    }

    public function protocolProvider(): iterable
    {
        // filename isParseable
        yield ['basic.avr', true];
        yield ['simple.avr', true];
        yield ['namespace.avr', true];
        yield ['implicit_namespace.avr', true];
        yield ['multiple_namespaces.avr', true];
        yield ['valid_repeated_name.avr', true];
        yield ['invalid_repeated_name.avr', false];
        yield ['bulk_data.avr', true];
        yield ['symbols.avr', true];
    }

    /**
     * @expectedException \Avro\Exception\ProtocolParseException
     * @expectedExceptionMessage Protocol can't be null
     */
    public function testMissingData(): void
    {
        Protocol::parse(null);
    }

    private function loadDataFromFile(string $filename): string
    {
        return file_get_contents(__DIR__.'/../Fixtures/Protocol/'.$filename);
    }
}
