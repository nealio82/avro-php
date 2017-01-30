<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use Avro\Util\Util;

/**
 * @package Avro
 */
class RecordSchema extends NamedSchema
{
    /**
     * @param mixed $field_data
     * @param string $default_namespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     * @returns Field[]
     * @throws SchemaParseException
     */
    static function parse_fields($field_data, $default_namespace, NamedSchemata &$schemata)
    {
        $fields = array();
        $field_names = array();
        foreach ($field_data as $index => $field) {
            $name = Util::array_value($field, Field::FIELD_NAME_ATTR);
            $type = Util::array_value($field, Schema::TYPE_ATTR);
            $order = Util::array_value($field, Field::ORDER_ATTR);

            $default = null;
            $has_default = false;
            if (array_key_exists(Field::DEFAULT_ATTR, $field)) {
                $default = $field[Field::DEFAULT_ATTR];
                $has_default = true;
            }

            if (in_array($name, $field_names))
                throw new SchemaParseException(
                    sprintf("Field name %s is already in use", $name));

            $is_schema_from_schemata = false;
            $field_schema = null;
            if (is_string($type)
                && $field_schema = $schemata->schema_by_name(
                    new Name($type, null, $default_namespace))
            )
                $is_schema_from_schemata = true;
            else
                $field_schema = self::subparse($type, $default_namespace, $schemata);

            $new_field = new Field($name, $field_schema, $is_schema_from_schemata,
                $has_default, $default, $order);
            $field_names [] = $name;
            $fields [] = $new_field;
        }
        return $fields;
    }

    /**
     * @var Schema[] array of NamedSchema field definitions of
     *                   this RecordSchema
     */
    private $fields;

    /**
     * @var array map of field names to field objects.
     * @internal Not called directly. Memoization of RecordSchema->fields_hash()
     */
    private $fields_hash;

    /**
     * @param string $name
     * @param string $namespace
     * @param string $doc
     * @param array $fields
     * @param NamedSchemata &$schemata
     * @param string $schema_type schema type name
     * @throws SchemaParseException
     */
    public function __construct($name, $doc, $fields, NamedSchemata &$schemata = null,
                                $schema_type = Schema::RECORD_SCHEMA)
    {
        if (is_null($fields)) {
            throw new SchemaParseException(
                'Record schema requires a non-empty fields attribute');
        }

        if (Schema::REQUEST_SCHEMA == $schema_type) {
            parent::__construct($schema_type, $name);
        } else {
            parent::__construct($schema_type, $name, $doc, $schemata);
        }

        list($x, $namespace) = $name->name_and_namespace();
        $this->fields = self::parse_fields($fields, $namespace, $schemata);
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();

        $fields_avro = array();
        foreach ($this->fields as $field)
            $fields_avro [] = $field->to_avro();

        if (Schema::REQUEST_SCHEMA == $this->type)
            return $fields_avro;

        $avro[Schema::FIELDS_ATTR] = $fields_avro;

        return $avro;
    }

    /**
     * @returns array the schema definitions of the fields of this RecordSchema
     */
    public function fields()
    {
        return $this->fields;
    }

    /**
     * @returns array a hash table of the fields of this RecordSchema fields
     *          keyed by each field's name
     */
    public function fields_hash()
    {
        if (is_null($this->fields_hash)) {
            $hash = array();
            foreach ($this->fields as $field)
                $hash[$field->name()] = $field;
            $this->fields_hash = $hash;
        }
        return $this->fields_hash;
    }
}