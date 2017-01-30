<?php

namespace Avro\Exception;

/**
 * Exceptions arising from incompatibility between
 * reader and writer schemas.
 *
 * @package Avro
 */
class IOSchemaMatchException extends Exception
{
    /**
     * @param AvroSchema $writers_schema
     * @param AvroSchema $readers_schema
     */
    function __construct($writers_schema, $readers_schema)
    {
        parent::__construct(
            sprintf("Writer's schema %s and Reader's schema %s do not match.",
                $writers_schema, $readers_schema));
    }
}