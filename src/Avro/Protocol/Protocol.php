<?php

namespace Avro\Protocol;

use Avro\Schema\NamedSchemata;
use Avro\Schema\Schema;

/**
 * Avro library for protocols.
 */
class Protocol
{
    /**
     * @var string
     */
    public $protocol;

    /**
     * @var string
     */
    public $namespace;

    /**
     * @var NamedSchemata
     */
    public $schemata;

    /**
     * @var string
     */
    public $name;

    /**
     * @var null|Schema
     */
    public $types;

    /**
     * @var null|ProtocolMessage[]
     */
    public $messages = [];

    public static function parse(string $json): self
    {
        $protocol = new self();
        $protocol->realParse(json_decode($json, true));

        return $protocol;
    }

    private function realParse(array $avro): void
    {
        $this->protocol = $avro['protocol'];
        $this->namespace = $avro['namespace'];
        $this->schemata = new NamedSchemata();
        $this->name = $avro['protocol'];

        if (null !== $avro['types']) {
            $this->types = Schema::realParse($avro['types'], $this->namespace, $this->schemata);
        }

        if (null !== $avro['messages']) {
            foreach ($avro['messages'] as $messageName => $messageAvro) {
                $message = new ProtocolMessage($messageName, $messageAvro, $this);
                $this->messages[$messageName] = $message;
            }
        }
    }
}
