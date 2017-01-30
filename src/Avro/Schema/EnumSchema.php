<?php

namespace Avro\Schema;

use Avro\Exception\Exception;
use Avro\Exception\SchemaParseException;
use Avro\Util\Util;

/**
 * @package Avro
 */
class EnumSchema extends NamedSchema
{
    /**
     * @var string[] array of symbols
     */
    private $symbols;

    /**
     * @param Name $name
     * @param string $doc
     * @param string[] $symbols
     * @param NamedSchemata &$schemata
     * @throws SchemaParseException
     */
    public function __construct(Name $name, $doc, $symbols, NamedSchemata &$schemata = null)
    {
        if (!Util::is_list($symbols))
            throw new SchemaParseException('Enum Schema symbols are not a list');

        if (count(array_unique($symbols)) > count($symbols))
            throw new SchemaParseException(
                sprintf('Duplicate symbols: %s', $symbols));

        foreach ($symbols as $symbol)
            if (!is_string($symbol) || empty($symbol))
                throw new SchemaParseException(
                    sprintf('Enum schema symbol must be a string %',
                        print_r($symbol, true)));

        parent::__construct(Schema::ENUM_SCHEMA, $name, $doc, $schemata);
        $this->symbols = $symbols;
    }

    /**
     * @returns string[] this enum schema's symbols
     */
    public function symbols()
    {
        return $this->symbols;
    }

    /**
     * @param string $symbol
     * @returns boolean true if the given symbol exists in this
     *          enum schema and false otherwise
     */
    public function has_symbol($symbol)
    {
        return in_array($symbol, $this->symbols);
    }

    /**
     * @param int $index
     * @returns string enum schema symbol with the given (zero-based) index
     */
    public function symbol_by_index($index)
    {
        if (array_key_exists($index, $this->symbols))
            return $this->symbols[$index];
        throw new Exception(sprintf('Invalid symbol index %d', $index));
    }

    /**
     * @param string $symbol
     * @returns int the index of the given $symbol in the enum schema
     */
    public function symbol_index($symbol)
    {
        $idx = array_search($symbol, $this->symbols, true);
        if (false !== $idx)
            return $idx;
        throw new Exception(sprintf("Invalid symbol value '%s'", $symbol));
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();
        $avro[Schema::SYMBOLS_ATTR] = $this->symbols;
        return $avro;
    }
}