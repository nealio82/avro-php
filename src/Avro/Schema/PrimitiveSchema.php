<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 * Avro schema for basic types such as null, int, long, string.
 * @package Avro
 */
class PrimitiveSchema extends Schema
{

    /**
     * @param string $type the primitive schema type name
     * @throws SchemaParseException if the given $type is not a
     *         primitive schema type name
     */
    public function __construct($type)
    {
        if (self::is_primitive_type($type))
            return parent::__construct($type);
        throw new SchemaParseException(
            sprintf('%s is not a valid primitive type.', $type));
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();
        // FIXME: Is this if really necessary? When *wouldn't* this be the case?
        if (1 == count($avro))
            return $this->type;
        return $avro;
    }
}