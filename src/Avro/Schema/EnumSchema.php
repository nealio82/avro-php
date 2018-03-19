<?php

namespace Avro\Schema;

use Avro\Exception\Exception;
use Avro\Exception\SchemaParseException;
use Avro\Util\Util;

class EnumSchema extends NamedSchema
{
    private $symbols;

    /**
     * @param string[] $symbols
     */
    public function __construct(Name $name, ?string $doc, array $symbols, NamedSchemata &$schemata = null)
    {
        if (!Util::isList($symbols)) {
            throw new SchemaParseException('Enum Schema symbols are not a list');
        }

        if (count(array_unique($symbols)) > count($symbols)) {
            throw new SchemaParseException(sprintf('Duplicate symbols: %s', $symbols));
        }

        foreach ($symbols as $symbol) {
            if (!is_string($symbol) || empty($symbol)) {
                throw new SchemaParseException(
                    sprintf('Enum schema symbol must be a string %s', print_r($symbol, true))
                );
            }
        }

        parent::__construct(Schema::ENUM_SCHEMA, $name, $doc, $schemata);

        $this->symbols = $symbols;
    }

    /**
     * @return string[]
     */
    public function symbols(): array
    {
        return $this->symbols;
    }

    public function hasSymbol(string $symbol): bool
    {
        return in_array($symbol, $this->symbols, true);
    }

    public function symbolByIndex(int $index): string
    {
        if (!array_key_exists($index, $this->symbols)) {
            throw new Exception(sprintf('Invalid symbol index %d', $index));
        }

        return $this->symbols[$index];
    }

    public function symbolIndex(string $symbol): int
    {
        $idx = array_search($symbol, $this->symbols, true);
        if (false === $idx) {
            throw new Exception(sprintf('Invalid symbol value "%s"', $symbol));
        }

        return $idx;
    }

    public function toAvro(): array
    {
        $avro = parent::toAvro();
        $avro[Schema::SYMBOLS_ATTR] = $this->symbols;

        return $avro;
    }
}
