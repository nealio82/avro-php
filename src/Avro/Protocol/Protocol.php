<?php

namespace Avro\Protocol;

use Avro\Exception\ProtocolParseException;
use Avro\Schema\NamedSchemata;
use Avro\Schema\Schema;

/**
 * Avro library for protocols
 * @package Avro
 */
class Protocol
{
    public $name;
    public $namespace;
    public $schemata;

    public static function parse($json)
    {
        if (is_null($json))
            throw new ProtocolParseException("Protocol can't be null");

        $protocol = new static();
        $protocol->real_parse(json_decode($json, true));
        return $protocol;
    }

    function real_parse($avro)
    {
        $this->protocol = $avro["protocol"];
        $this->namespace = $avro["namespace"];
        $this->schemata = new NamedSchemata();
        $this->name = $avro["protocol"];

        if (!is_null($avro["types"])) {
            $types = Schema::real_parse($avro["types"], $this->namespace, $this->schemata);
        }

        if (!is_null($avro["messages"])) {
            foreach ($avro["messages"] as $messageName => $messageAvro) {
                $message = new ProtocolMessage($messageName, $messageAvro, $this);
                $this->messages{$messageName} = $message;
            }
        }
    }
}