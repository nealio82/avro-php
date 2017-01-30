<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 * Union of Avro schemas, of which values can be of any of the schema in
 * the union.
 * @package Avro
 */
class UnionSchema extends Schema
{
    /**
     * @var Schema[] list of schemas of this union
     */
    private $schemas;

    /**
     * @var int[] list of indices of named schemas which
     *                are defined in $schemata
     */
    public $schema_from_schemata_indices;

    /**
     * @param Schema[] $schemas list of schemas in the union
     * @param string $default_namespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     */
    public function __construct($schemas, $default_namespace, NamedSchemata &$schemata = null)
    {
        parent::__construct(Schema::UNION_SCHEMA);

        $this->schema_from_schemata_indices = array();
        $schema_types = array();
        foreach ($schemas as $index => $schema) {
            $is_schema_from_schemata = false;
            $new_schema = null;
            if (is_string($schema)
                && ($new_schema = $schemata->schema_by_name(
                    new Name($schema, null, $default_namespace)))
            )
                $is_schema_from_schemata = true;
            else
                $new_schema = self::subparse($schema, $default_namespace, $schemata);

            $schema_type = $new_schema->type;
            if (self::is_valid_type($schema_type)
                && !self::is_named_type($schema_type)
                && in_array($schema_type, $schema_types)
            )
                throw new SchemaParseException(
                    sprintf('"%s" is already in union', $schema_type));
            elseif (Schema::UNION_SCHEMA == $schema_type)
                throw new SchemaParseException('Unions cannot contain other unions');
            else {
                $schema_types [] = $schema_type;
                $this->schemas [] = $new_schema;
                if ($is_schema_from_schemata)
                    $this->schema_from_schemata_indices [] = $index;
            }
        }

    }

    /**
     * @returns Schema[]
     */
    public function schemas()
    {
        return $this->schemas;
    }

    /**
     * @returns Schema the particular schema from the union for
     * the given (zero-based) index.
     * @throws SchemaParseException if the index is invalid for this schema.
     */
    public function schema_by_index($index)
    {
        if (count($this->schemas) > $index)
            return $this->schemas[$index];

        throw new SchemaParseException('Invalid union schema index');
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = array();

        foreach ($this->schemas as $index => $schema)
            $avro [] = (in_array($index, $this->schema_from_schemata_indices))
                ? $schema->qualified_name() : $schema->to_avro();

        return $avro;
    }
}