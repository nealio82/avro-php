<?php

namespace Avro\Exception;

use Avro\Schema\Schema;

/**
 * Exceptions arising from writing or reading Avro data.
 */
class IOTypeException extends Exception
{
    /**
     * @param mixed $datum
     */
    public function __construct(Schema $expectedSchema, $datum)
    {
        parent::__construct(
            sprintf('The datum %s is not an example of schema %s', var_export($datum, true), $expectedSchema)
        );
    }
}
