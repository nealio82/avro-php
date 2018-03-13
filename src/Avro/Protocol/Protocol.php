<?php

namespace Avro\Protocol;

use Avro\Exception\ProtocolParseException;
use Avro\Schema\NamedSchemata;
use Avro\Schema\Schema;

/**
 * Avro library for protocols.
 */
class Protocol
{
    public $name;
    public $namespace;
    public $schemata;

    public static function parse($json)
    {
        if (null === $json) {
            throw new ProtocolParseException("Protocol can't be null");
        }
        $protocol = new static();
        $protocol->real_parse(json_decode($json, true));

        return $protocol;
    }

    public function real_parse($avro): void
    {
        $this->protocol = $avro['protocol'];
        $this->namespace = $avro['namespace'];
        $this->schemata = new NamedSchemata();
        $this->name = $avro['protocol'];

        if (null !== $avro['types']) {
            $this->types = Schema::real_parse($avro['types'], $this->namespace, $this->schemata);
        }

        if (null !== $avro['messages']) {
            foreach ($avro['messages'] as $messageName => $messageAvro) {
                $message = new ProtocolMessage($messageName, $messageAvro, $this);
                $this->messages[$messageName] = $message;
            }
        }
    }
}
