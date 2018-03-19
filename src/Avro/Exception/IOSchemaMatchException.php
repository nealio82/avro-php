<?php

namespace Avro\Exception;

use Avro\Schema\Schema;

/**
 * Exceptions arising from incompatibility between reader and writer schemas.
 */
class IOSchemaMatchException extends Exception
{
    public function __construct(Schema $writersSchema, Schema $readersSchema)
    {
        parent::__construct(
            sprintf('Writer\'s schema %s and Reader\'s schema %s do not match.', $writersSchema, $readersSchema)
        );
    }
}
