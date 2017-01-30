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
     * IOSchemaMatchException constructor.
     * @param string $writers_schema
     * @param int $readers_schema
     */
    function __construct($writers_schema, $readers_schema)
    {
        parent::__construct(
            sprintf("Writer's schema %s and Reader's schema %s do not match.",
                $writers_schema, $readers_schema));
    }
}