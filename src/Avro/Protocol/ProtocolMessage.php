<?php

namespace Avro\Protocol;

use Avro\Schema\Name;
use Avro\Schema\RecordSchema;
use Avro\Schema\Schema;

class ProtocolMessage
{
    /**
     * @var RecordSchema
     */
    public $request;

    public $response;

    public function __construct($name, $avro, Protocol $protocol)
    {
        $this->name = $name;
        $this->request = new RecordSchema(new Name($name, null, $protocol->namespace), null, $avro['request'], $protocol->schemata, Schema::REQUEST_SCHEMA);

        if (array_key_exists('response', $avro)) {
            $this->response = $protocol->schemata->schema_by_name(new Name($avro['response'], $protocol->namespace, $protocol->namespace));
            if (null == $this->response) {
                $this->response = new PrimitiveSchema($avro['response']);
            }
        }
    }
}
