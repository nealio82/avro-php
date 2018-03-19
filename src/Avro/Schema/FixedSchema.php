<?php

namespace Avro\Schema;

/**
 * NamedSchema with fixed-length data values.
 */
class FixedSchema extends NamedSchema
{
    private $size;

    public function __construct(Name $name, ?string $doc, int $size, NamedSchemata &$schemata = null)
    {
        parent::__construct(Schema::FIXED_SCHEMA, $name, null, $schemata);

        $this->size = $size;
    }

    public function size(): int
    {
        return $this->size;
    }

    public function toAvro(): array
    {
        $avro = parent::toAvro();
        $avro[Schema::SIZE_ATTR] = $this->size;

        return $avro;
    }
}
