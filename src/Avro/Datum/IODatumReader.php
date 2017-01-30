<?php

namespace Avro\Datum;

use Avro\Exception\Exception;
use Avro\Exception\IOSchemaMatchException;
use Avro\Schema\Schema;

/**
 * Handles schema-specifc reading of data from the decoder.
 *
 * Also handles schema resolution between the reader and writer
 * schemas (if a writer's schema is provided).
 *
 * @package Avro
 */
class IODatumReader
{
    /**
     *
     * @param Schema $writers_schema
     * @param Schema $readers_schema
     * @returns boolean true if the schemas are consistent with
     *                  each other and false otherwise.
     */
    static function schemas_match(Schema $writers_schema, Schema $readers_schema)
    {
        $writers_schema_type = $writers_schema->type;
        $readers_schema_type = $readers_schema->type;

        if (Schema::UNION_SCHEMA == $writers_schema_type
            || Schema::UNION_SCHEMA == $readers_schema_type
        )
            return true;

        if ($writers_schema_type == $readers_schema_type) {
            if (Schema::is_primitive_type($writers_schema_type))
                return true;

            switch ($readers_schema_type) {
                case Schema::MAP_SCHEMA:
                    return self::attributes_match($writers_schema->values(),
                        $readers_schema->values(),
                        array(Schema::TYPE_ATTR));
                case Schema::ARRAY_SCHEMA:
                    return self::attributes_match($writers_schema->items(),
                        $readers_schema->items(),
                        array(Schema::TYPE_ATTR));
                case Schema::ENUM_SCHEMA:
                    return self::attributes_match($writers_schema, $readers_schema,
                        array(Schema::FULLNAME_ATTR));
                case Schema::FIXED_SCHEMA:
                    return self::attributes_match($writers_schema, $readers_schema,
                        array(Schema::FULLNAME_ATTR,
                            Schema::SIZE_ATTR));
                case Schema::RECORD_SCHEMA:
                case Schema::ERROR_SCHEMA:
                    return self::attributes_match($writers_schema, $readers_schema,
                        array(Schema::FULLNAME_ATTR));
                case Schema::REQUEST_SCHEMA:
                    // XXX: This seems wrong
                    return true;
                // XXX: no default
            }

            if (Schema::INT_TYPE == $writers_schema_type
                && in_array($readers_schema_type, array(Schema::LONG_TYPE,
                    Schema::FLOAT_TYPE,
                    Schema::DOUBLE_TYPE))
            )
                return true;

            if (Schema::LONG_TYPE == $writers_schema_type
                && in_array($readers_schema_type, array(Schema::FLOAT_TYPE,
                    Schema::DOUBLE_TYPE))
            )
                return true;

            if (Schema::FLOAT_TYPE == $writers_schema_type
                && Schema::DOUBLE_TYPE == $readers_schema_type
            )
                return true;

            return false;
        }

    }

    /**
     * Checks equivalence of the given attributes of the two given schemas.
     *
     * @param Schema $schema_one
     * @param Schema $schema_two
     * @param string[] $attribute_names array of string attribute names to compare
     *
     * @returns boolean true if the attributes match and false otherwise.
     */
    static function attributes_match(Schema $schema_one, Schema $schema_two, $attribute_names)
    {
        foreach ($attribute_names as $attribute_name)
            if ($schema_one->attribute($attribute_name)
                != $schema_two->attribute($attribute_name)
            )
                return false;
        return true;
    }

    /**
     * @var Schema
     */
    private $writers_schema;

    /**
     * @var Schema
     */
    private $readers_schema;

    /**
     * @param Schema $writers_schema
     * @param Schema $readers_schema
     */
    function __construct(Schema $writers_schema = null, Schema $readers_schema = null)
    {
        $this->writers_schema = $writers_schema;
        $this->readers_schema = $readers_schema;
    }

    /**
     * @param Schema $readers_schema
     */
    public function set_writers_schema(Schema $readers_schema)
    {
        $this->writers_schema = $readers_schema;
    }

    /**
     * @param IOBinaryDecoder $decoder
     * @returns string
     */
    public function read(IOBinaryDecoder $decoder)
    {
        if (is_null($this->readers_schema))
            $this->readers_schema = $this->writers_schema;
        return $this->read_data($this->writers_schema, $this->readers_schema,
            $decoder);
    }

    /**#@+
     * @param Schema $writers_schema
     * @param Schema $readers_schema
     * @param IOBinaryDecoder $decoder
     */
    /**
     * @returns mixed
     */
    public function read_data(Schema $writers_schema, Schema $readers_schema, IOBinaryDecoder $decoder)
    {
        if (!self::schemas_match($writers_schema, $readers_schema))
            throw new IOSchemaMatchException($writers_schema, $readers_schema);

        // Schema resolution: reader's schema is a union, writer's schema is not
        if (Schema::UNION_SCHEMA == $readers_schema->type()
            && Schema::UNION_SCHEMA != $writers_schema->type()
        ) {
            foreach ($readers_schema->schemas() as $schema)
                if (self::schemas_match($writers_schema, $schema))
                    return $this->read_data($writers_schema, $schema, $decoder);
            throw new IOSchemaMatchException($writers_schema, $readers_schema);
        }

        switch ($writers_schema->type()) {
            case Schema::NULL_TYPE:
                return $decoder->read_null();
            case Schema::BOOLEAN_TYPE:
                return $decoder->read_boolean();
            case Schema::INT_TYPE:
                return $decoder->read_int();
            case Schema::LONG_TYPE:
                return $decoder->read_long();
            case Schema::FLOAT_TYPE:
                return $decoder->read_float();
            case Schema::DOUBLE_TYPE:
                return $decoder->read_double();
            case Schema::STRING_TYPE:
                return $decoder->read_string();
            case Schema::BYTES_TYPE:
                return $decoder->read_bytes();
            case Schema::ARRAY_SCHEMA:
                return $this->read_array($writers_schema, $readers_schema, $decoder);
            case Schema::MAP_SCHEMA:
                return $this->read_map($writers_schema, $readers_schema, $decoder);
            case Schema::UNION_SCHEMA:
                return $this->read_union($writers_schema, $readers_schema, $decoder);
            case Schema::ENUM_SCHEMA:
                return $this->read_enum($writers_schema, $readers_schema, $decoder);
            case Schema::FIXED_SCHEMA:
                return $this->read_fixed($writers_schema, $readers_schema, $decoder);
            case Schema::RECORD_SCHEMA:
            case Schema::ERROR_SCHEMA:
            case Schema::REQUEST_SCHEMA:
                return $this->read_record($writers_schema, $readers_schema, $decoder);
            default:
                throw new Exception(sprintf("Cannot read unknown schema type: %s",
                    $writers_schema->type()));
        }
    }

    /**
     * @returns array
     */
    public function read_array(Schema $writers_schema, Schema $readers_schema, IOBinaryDecoder $decoder)
    {
        $items = array();
        $block_count = $decoder->read_long();
        while (0 != $block_count) {
            if ($block_count < 0) {
                $block_count = -$block_count;
                $block_size = $decoder->read_long(); // Read (and ignore) block size
            }
            for ($i = 0; $i < $block_count; $i++)
                $items [] = $this->read_data($writers_schema->items(),
                    $readers_schema->items(),
                    $decoder);
            $block_count = $decoder->read_long();
        }
        return $items;
    }

    /**
     * @returns array
     */
    public function read_map(Schema $writers_schema, Schema $readers_schema, IOBinaryDecoder $decoder)
    {
        $items = array();
        $pair_count = $decoder->read_long();
        while (0 != $pair_count) {
            if ($pair_count < 0) {
                $pair_count = -$pair_count;
                // Note: we're not doing anything with block_size other than skipping it
                $block_size = $decoder->read_long();
            }

            for ($i = 0; $i < $pair_count; $i++) {
                $key = $decoder->read_string();

                $items[$key] = $this->read_data($writers_schema->values(),
                    $readers_schema->values(),
                    $decoder);
            }
            $pair_count = $decoder->read_long();
        }
        return $items;
    }

    /**
     * @returns mixed
     */
    public function read_union(Schema $writers_schema, Schema $readers_schema, IOBinaryDecoder $decoder)
    {
        $schema_index = $decoder->read_long();

        $selected_writers_schema = $writers_schema->schema_by_index($schema_index);
        return $this->read_data($selected_writers_schema, $readers_schema, $decoder);
    }

    /**
     * @returns string
     */
    public function read_enum(Schema $writers_schema, Schema $readers_schema, IOBinaryDecoder $decoder)
    {
        $symbol_index = $decoder->read_int();
        $symbol = $writers_schema->symbol_by_index($symbol_index);
        if (!$readers_schema->has_symbol($symbol))
            null; // FIXME: unset wrt schema resolution
        return $symbol;
    }

    /**
     * @returns string
     */
    public function read_fixed(Schema $writers_schema, Schema $readers_schema, IOBinaryDecoder $decoder)
    {
        return $decoder->read($writers_schema->size());
    }

    /**
     * @returns array
     */
    public function read_record(Schema $writers_schema, Schema $readers_schema, IOBinaryDecoder $decoder)
    {
        $readers_fields = $readers_schema->fields_hash();
        $record = array();
        foreach ($writers_schema->fields() as $writers_field) {
            $type = $writers_field->type();
            if (isset($readers_fields[$writers_field->name()]))
                $record[$writers_field->name()]
                    = $this->read_data($type,
                    $readers_fields[$writers_field->name()]->type(),
                    $decoder);
            else
                $this->skip_data($type, $decoder);
        }
        // Fill in default values
        if (count($readers_fields) > count($record)) {
            $writers_fields = $writers_schema->fields_hash();
            foreach ($readers_fields as $field_name => $field) {
                if (!isset($writers_fields[$field_name])) {
                    if ($field->has_default_value())
                        $record[$field->name()]
                            = $this->read_default_value($field->type(),
                            $field->default_value());
                    else
                        null; // FIXME: unset
                }
            }
        }

        return $record;
    }
    /**#@-*/

    /**
     * @param Schema $field_schema
     * @param null|boolean|int|float|string|array $default_value
     * @returns null|boolean|int|float|string|array
     *
     * @throws Exception if $field_schema type is unknown.
     */
    public function read_default_value(Schema $field_schema, $default_value)
    {
        switch ($field_schema->type()) {
            case Schema::NULL_TYPE:
                return null;
            case Schema::BOOLEAN_TYPE:
                return $default_value;
            case Schema::INT_TYPE:
            case Schema::LONG_TYPE:
                return (int)$default_value;
            case Schema::FLOAT_TYPE:
            case Schema::DOUBLE_TYPE:
                return (float)$default_value;
            case Schema::STRING_TYPE:
            case Schema::BYTES_TYPE:
                return $default_value;
            case Schema::ARRAY_SCHEMA:
                $array = array();
                foreach ($default_value as $json_val) {
                    $val = $this->read_default_value($field_schema->items(), $json_val);
                    $array [] = $val;
                }
                return $array;
            case Schema::MAP_SCHEMA:
                $map = array();
                foreach ($default_value as $key => $json_val)
                    $map[$key] = $this->read_default_value($field_schema->values(),
                        $json_val);
                return $map;
            case Schema::UNION_SCHEMA:
                return $this->read_default_value($field_schema->schema_by_index(0),
                    $default_value);
            case Schema::ENUM_SCHEMA:
            case Schema::FIXED_SCHEMA:
                return $default_value;
            case Schema::RECORD_SCHEMA:
                $record = array();
                foreach ($field_schema->fields() as $field) {
                    $field_name = $field->name();
                    if (!$json_val = $default_value[$field_name])
                        $json_val = $field->default_value();

                    $record[$field_name] = $this->read_default_value($field->type(),
                        $json_val);
                }
                return $record;
            default:
                throw new Exception(sprintf('Unknown type: %s', $field_schema->type()));
        }
    }

    /**
     * @param Schema $writers_schema
     * @param IOBinaryDecoder $decoder
     */
    private function skip_data(Schema $writers_schema, IOBinaryDecoder $decoder)
    {
        switch ($writers_schema->type()) {
            case Schema::NULL_TYPE:
                return $decoder->skip_null();
            case Schema::BOOLEAN_TYPE:
                return $decoder->skip_boolean();
            case Schema::INT_TYPE:
                return $decoder->skip_int();
            case Schema::LONG_TYPE:
                return $decoder->skip_long();
            case Schema::FLOAT_TYPE:
                return $decoder->skip_float();
            case Schema::DOUBLE_TYPE:
                return $decoder->skip_double();
            case Schema::STRING_TYPE:
                return $decoder->skip_string();
            case Schema::BYTES_TYPE:
                return $decoder->skip_bytes();
            case Schema::ARRAY_SCHEMA:
                return $decoder->skip_array($writers_schema, $decoder);
            case Schema::MAP_SCHEMA:
                return $decoder->skip_map($writers_schema, $decoder);
            case Schema::UNION_SCHEMA:
                return $decoder->skip_union($writers_schema, $decoder);
            case Schema::ENUM_SCHEMA:
                return $decoder->skip_enum($writers_schema, $decoder);
            case Schema::FIXED_SCHEMA:
                return $decoder->skip_fixed($writers_schema, $decoder);
            case Schema::RECORD_SCHEMA:
            case Schema::ERROR_SCHEMA:
            case Schema::REQUEST_SCHEMA:
                return $decoder->skip_record($writers_schema, $decoder);
            default:
                throw new Exception(sprintf('Uknown schema type: %s',
                    $writers_schema->type()));
        }
    }
}