<?php

namespace Avro\Datum;

use Avro\Exception\Exception;
use Avro\Exception\IOTypeException;
use Avro\Schema\Schema;

/**
 * Handles schema-specific writing of data to the encoder.
 *
 * Ensures that each datum written is consistent with the writer's schema.
 *
 * @package Avro
 */
class IODatumWriter
{
    /**
     * Schema used by this instance to write Avro data.
     * @var Schema
     */
    private $writers_schema;

    /**
     * @param Schema $writers_schema
     */
    function __construct(Schema $writers_schema = null)
    {
        $this->writers_schema = $writers_schema;
    }

    /**
     * @param Schema $writers_schema
     * @param $datum
     * @param IOBinaryEncoder $encoder
     * @returns mixed
     *
     * @throws IOTypeException if $datum is invalid for $writers_schema
     */
    function write_data(Schema $writers_schema, $datum, IOBinaryEncoder $encoder)
    {
        if (!Schema::is_valid_datum($writers_schema, $datum))
            throw new IOTypeException($writers_schema, $datum);

        switch ($writers_schema->type()) {
            case Schema::NULL_TYPE:
                return $encoder->write_null($datum);
            case Schema::BOOLEAN_TYPE:
                return $encoder->write_boolean($datum);
            case Schema::INT_TYPE:
                return $encoder->write_int($datum);
            case Schema::LONG_TYPE:
                return $encoder->write_long($datum);
            case Schema::FLOAT_TYPE:
                return $encoder->write_float($datum);
            case Schema::DOUBLE_TYPE:
                return $encoder->write_double($datum);
            case Schema::STRING_TYPE:
                return $encoder->write_string($datum);
            case Schema::BYTES_TYPE:
                return $encoder->write_bytes($datum);
            case Schema::ARRAY_SCHEMA:
                return $this->write_array($writers_schema, $datum, $encoder);
            case Schema::MAP_SCHEMA:
                return $this->write_map($writers_schema, $datum, $encoder);
            case Schema::FIXED_SCHEMA:
                return $this->write_fixed($writers_schema, $datum, $encoder);
            case Schema::ENUM_SCHEMA:
                return $this->write_enum($writers_schema, $datum, $encoder);
            case Schema::RECORD_SCHEMA:
            case Schema::ERROR_SCHEMA:
            case Schema::REQUEST_SCHEMA:
                return $this->write_record($writers_schema, $datum, $encoder);
            case Schema::UNION_SCHEMA:
                return $this->write_union($writers_schema, $datum, $encoder);
            default:
                throw new Exception(sprintf('Uknown type: %s',
                    $writers_schema->type));
        }
    }

    /**
     * @param $datum
     * @param IOBinaryEncoder $encoder
     */
    function write($datum, IOBinaryEncoder $encoder)
    {
        $this->write_data($this->writers_schema, $datum, $encoder);
    }

    /**#@+
     * @param Schema $writers_schema
     * @param null|boolean|int|float|string|array $datum item to be written
     * @param IOBinaryEncoder $encoder
     */
    private function write_array(Schema $writers_schema, $datum, IOBinaryEncoder $encoder)
    {
        $datum_count = count($datum);
        if (0 < $datum_count) {
            $encoder->write_long($datum_count);
            $items = $writers_schema->items();
            foreach ($datum as $item)
                $this->write_data($items, $item, $encoder);
        }
        return $encoder->write_long(0);
    }

    private function write_map(Schema $writers_schema, $datum, IOBinaryEncoder $encoder)
    {
        $datum_count = count($datum);
        if ($datum_count > 0) {
            $encoder->write_long($datum_count);
            foreach ($datum as $k => $v) {
                $encoder->write_string($k);
                $this->write_data($writers_schema->values(), $v, $encoder);
            }
        }
        $encoder->write_long(0);
    }

    private function write_union(Schema $writers_schema, $datum, IOBinaryEncoder $encoder)
    {
        $datum_schema_index = -1;
        $datum_schema = null;
        foreach ($writers_schema->schemas() as $index => $schema)
            if (Schema::is_valid_datum($schema, $datum)) {
                $datum_schema_index = $index;
                $datum_schema = $schema;
                break;
            }

        if (is_null($datum_schema))
            throw new IOTypeException($writers_schema, $datum);

        $encoder->write_long($datum_schema_index);
        $this->write_data($datum_schema, $datum, $encoder);
    }

    private function write_enum(Schema $writers_schema, $datum, IOBinaryEncoder $encoder)
    {
        $datum_index = $writers_schema->symbol_index($datum);
        return $encoder->write_int($datum_index);
    }

    private function write_fixed(Schema $writers_schema, $datum, IOBinaryEncoder $encoder)
    {
        /**
         * NOTE Unused $writers_schema parameter included for consistency
         * with other write_* methods.
         */
        return $encoder->write($datum);
    }

    private function write_record(Schema $writers_schema, $datum, IOBinaryEncoder $encoder)
    {
        foreach ($writers_schema->fields() as $field)
            $this->write_data($field->type(), $datum[$field->name()], $encoder);
    }

    /**#@-*/
}