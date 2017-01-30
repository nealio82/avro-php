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
     * IOTypeException constructor.
     * @param string $expected_schema
     * @param int $datum
     */
    public function __construct($expected_schema, $datum)
    {
        parent::__construct(sprintf('The datum %s is not an example of schema %s',
            var_export($datum, true), $expected_schema));
    }
}