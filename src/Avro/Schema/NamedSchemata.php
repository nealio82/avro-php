<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 *  Keeps track of NamedSchema which have been observed so far,
 *  as well as the default namespace.
 *
 * @package Avro
 */
class NamedSchemata
{
    /**
     * @var NamedSchema[]
     */
    private $schemata;

    /**
     * @param NamedSchemata []
     */
    public function __construct($schemata = array())
    {
        $this->schemata = $schemata;
    }

    public function list_schemas()
    {
        var_export($this->schemata);
        foreach ($this->schemata as $sch)
            print('Schema ' . $sch->__toString() . "\n");
    }

    /**
     * @param string $fullname
     * @returns boolean true if there exists a schema with the given name
     *                  and false otherwise.
     */
    public function has_name($fullname)
    {
        return array_key_exists($fullname, $this->schemata);
    }

    /**
     * @param string $fullname
     * @returns Schema|null the schema which has the given name,
     *          or null if there is no schema with the given name.
     */
    public function schema($fullname)
    {
        if (isset($this->schemata[$fullname]))
            return $this->schemata[$fullname];
        return null;
    }

    /**
     * @param Name $name
     * @returns Schema|null
     */
    public function schema_by_name(Name $name)
    {
        return $this->schema($name->fullname());
    }

    /**
     * Creates a new NamedSchemata instance of this schemata instance
     * with the given $schema appended.
     * @param NamedSchema schema to add to this existing schemata
     * @returns NamedSchemata
     */
    public function clone_with_new_schema(NamedSchema $schema)
    {
        $name = $schema->fullname();
        if (Schema::is_valid_type($name)) {
            throw new SchemaParseException(
                sprintf('Name "%s" is a reserved type name', $name));
        } else if ($this->has_name($name)) {
            throw new SchemaParseException(
                sprintf('Name "%s" is already in use', $name));
        }
        $schemata = new NamedSchemata($this->schemata);
        $schemata->schemata[$name] = $schema;
        return $schemata;
    }
}