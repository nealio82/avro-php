<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 * NamedSchema with fixed-length data values
 * @package Avro
 */
class FixedSchema extends NamedSchema
{

    /**
     * @var int byte count of this fixed schema data value
     */
    private $size;

    /**
     * @param Name $name
     * @param string $doc Set to null, as fixed schemas don't have doc strings
     * @param int $size byte count of this fixed schema data value
     * @param NamedSchemata &$schemata
     */
    public function __construct(Name $name, $doc, $size, NamedSchemata &$schemata = null)
    {
        $doc = null; // Fixed schemas don't have doc strings.
        if (!is_integer($size)) {
            throw new SchemaParseException(
                'Fixed Schema requires a valid integer for "size" attribute');
        }
        parent::__construct(Schema::FIXED_SCHEMA, $name, $doc, $schemata);
        return $this->size = $size;
    }

    /**
     * @returns int byte count of this fixed schema data value
     */
    public function size()
    {
        return $this->size;
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();
        $avro[Schema::SIZE_ATTR] = $this->size;
        return $avro;
    }
}