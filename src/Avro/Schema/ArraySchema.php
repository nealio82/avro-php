<?php

namespace Avro\Schema;

/**
 * Avro array schema, consisting of items of a particular
 * Avro schema type.
 * @package Avro
 */
class ArraySchema extends Schema
{
    /**
     * @var Name|Schema named schema name or Schema of array element
     */
    private $items;

    /**
     * @var boolean true if the items schema
     * FIXME: couldn't we derive this from whether or not $this->items
     *        is an Name or an Schema?
     */
    private $is_items_schema_from_schemata;

    /**
     * @param string|mixed $items NamedSchema name or object form
     *        of decoded JSON schema representation.
     * @param string $default_namespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     */
    public function __construct($items, $default_namespace, NamedSchemata &$schemata = null)
    {
        parent::__construct(Schema::ARRAY_SCHEMA);

        $this->is_items_schema_from_schemata = false;
        $items_schema = null;
        if (is_string($items)
            && $items_schema = $schemata->schema_by_name(
                new Name($items, null, $default_namespace))
        )
            $this->is_items_schema_from_schemata = true;
        else
            $items_schema = Schema::subparse($items, $default_namespace, $schemata);

        $this->items = $items_schema;
    }


    /**
     * @returns Name|Schema named schema name or Schema
     *          of this array schema's elements.
     */
    public function items()
    {
        return $this->items;
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();
        $avro[Schema::ITEMS_ATTR] = $this->is_items_schema_from_schemata
            ? $this->items->qualified_name() : $this->items->to_avro();
        return $avro;
    }
}