<?php

namespace Avro\Exception;

/**
 * Exceptions arising from writing or reading Avro data.
 *
 * @package Avro
 */
class IOTypeException extends Exception
{
    /**
     * @param AvroSchema $expected_schema
     * @param mixed $datum
     */
    public function __construct($expected_schema, $datum)
    {
        parent::__construct(sprintf('The datum %s is not an example of schema %s',
            var_export($datum, true), $expected_schema));
    }
}