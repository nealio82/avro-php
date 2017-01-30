<?php

namespace Avro\Schema;

/**
 * Avro map schema consisting of named values of defined
 * Avro Schema types.
 * @package Avro
 */
class MapSchema extends Schema
{
    /**
     * @var string|Schema named schema name or Schema
     *      of map schema values.
     */
    private $values;

    /**
     * @var boolean true if the named schema
     * XXX Couldn't we derive this based on whether or not
     * $this->values is a string?
     */
    private $is_values_schema_from_schemata;

    /**
     * @param string|Schema $values
     * @param string $default_namespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     */
    public function __construct($values, $default_namespace, NamedSchemata &$schemata = null)
    {
        parent::__construct(Schema::MAP_SCHEMA);

        $this->is_values_schema_from_schemata = false;
        $values_schema = null;
        if (is_string($values)
            && $values_schema = $schemata->schema_by_name(
                new Name($values, null, $default_namespace))
        )
            $this->is_values_schema_from_schemata = true;
        else
            $values_schema = Schema::subparse($values, $default_namespace,
                $schemata);

        $this->values = $values_schema;
    }

    /**
     * @returns XXX|Schema
     */
    public function values()
    {
        return $this->values;
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();
        $avro[Schema::VALUES_ATTR] = $this->is_values_schema_from_schemata
            ? $this->values->qualified_name() : $this->values->to_avro();
        return $avro;
    }
}