<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 * Avro schema for basic types such as null, int, long, string.
 */
class PrimitiveSchema extends Schema
{
    /**
     * @param mixed $type
     */
    public function __construct($type)
    {
        if (!self::isPrimitiveType($type)) {
            throw new SchemaParseException(sprintf('%s is not a valid primitive type.', $type));
        }

        parent::__construct($type);
    }

    public function toAvro()
    {
        $avro = parent::toAvro();
        // @todo Is this if really necessary? When *wouldn't* this be the case?
        if (1 === count($avro)) {
            return $this->type;
        }

        return $avro;
    }
}
