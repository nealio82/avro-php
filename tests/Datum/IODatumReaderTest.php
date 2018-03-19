<?php

namespace Avro\Datum;

use Avro\Schema\Schema;
use PHPUnit\Framework\TestCase;

class IODatumReaderTest extends TestCase
{
    public function testSchemaMatching(): void
    {
        $writersSchema = <<<JSON
{
    "type": "map",
    "values": "bytes"
}
JSON;
        $readersSchema = $writersSchema;
        $this->assertTrue(
            IODatumReader::schemasMatch(
                Schema::parse($writersSchema),
                Schema::parse($readersSchema)
            )
        );
    }
}
