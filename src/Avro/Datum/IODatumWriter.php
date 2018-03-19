<?php

namespace Avro\Datum;

use Avro\Exception\Exception;
use Avro\Exception\IOTypeException;
use Avro\Schema\ArraySchema;
use Avro\Schema\EnumSchema;
use Avro\Schema\FixedSchema;
use Avro\Schema\MapSchema;
use Avro\Schema\RecordSchema;
use Avro\Schema\Schema;
use Avro\Schema\UnionSchema;

/**
 * Handles schema-specific writing of data to the encoder.
 * Ensures that each datum written is consistent with the writer's schema.
 */
class IODatumWriter
{
    /**
     * Schema used by this instance to write Avro data.
     */
    private $writersSchema;

    public function __construct(?Schema $writersSchema = null)
    {
        $this->writersSchema = $writersSchema;
    }

    public function writeData(Schema $writersSchema, $datum, IOBinaryEncoder $encoder): void
    {
        if (!Schema::isValidDatum($writersSchema, $datum)) {
            throw new IOTypeException($writersSchema, $datum);
        }

        switch ($writersSchema->type()) {
            case Schema::NULL_TYPE:
                $encoder->writeNull($datum);
                break;
            case Schema::BOOLEAN_TYPE:
                $encoder->writeBoolean($datum);
                break;
            case Schema::INT_TYPE:
                $encoder->writeInt($datum);
                break;
            case Schema::LONG_TYPE:
                $encoder->writeLong($datum);
                break;
            case Schema::FLOAT_TYPE:
                $encoder->writeFloat($datum);
                break;
            case Schema::DOUBLE_TYPE:
                $encoder->writeDouble($datum);
                break;
            case Schema::STRING_TYPE:
                $encoder->writeString($datum);
                break;
            case Schema::BYTES_TYPE:
                $encoder->writeBytes($datum);
                break;
            case Schema::ARRAY_SCHEMA:
                $this->writeArray($writersSchema, $datum, $encoder);
                break;
            case Schema::MAP_SCHEMA:
                $this->writeMap($writersSchema, $datum, $encoder);
                break;
            case Schema::FIXED_SCHEMA:
                $this->writeFixed($writersSchema, $datum, $encoder);
                break;
            case Schema::ENUM_SCHEMA:
                $this->writeEnum($writersSchema, $datum, $encoder);
                break;
            case Schema::RECORD_SCHEMA:
            case Schema::ERROR_SCHEMA:
            case Schema::REQUEST_SCHEMA:
                $this->writeRecord($writersSchema, $datum, $encoder);
                break;
            case Schema::UNION_SCHEMA:
                $this->writeUnion($writersSchema, $datum, $encoder);
                break;
            default:
                throw new Exception(sprintf('Uknown type: %s', $writersSchema->type));
        }
    }

    /**
     * @param mixed $datum
     */
    public function write($datum, IOBinaryEncoder $encoder): void
    {
        $this->writeData($this->writersSchema, $datum, $encoder);
    }

    /**
     * @param Schema|ArraySchema $writersSchema
     * @param mixed              $datum
     */
    private function writeArray(Schema $writersSchema, $datum, IOBinaryEncoder $encoder): void
    {
        $datumCount = count($datum);
        if (0 < $datumCount) {
            $encoder->writeLong($datumCount);
            $items = $writersSchema->items();
            foreach ($datum as $item) {
                $this->writeData($items, $item, $encoder);
            }
        }

        $encoder->writeLong(0);
    }

    /**
     * @param Schema|MapSchema $writersSchema
     * @param mixed            $datum
     */
    private function writeMap(Schema $writersSchema, $datum, IOBinaryEncoder $encoder): void
    {
        $datumCount = count($datum);
        if ($datumCount > 0) {
            $encoder->writeLong($datumCount);
            foreach ($datum as $key => $value) {
                $encoder->writeString($key);
                $this->writeData($writersSchema->values(), $value, $encoder);
            }
        }

        $encoder->writeLong(0);
    }

    /**
     * @param Schema|UnionSchema $writersSchema
     * @param mixed              $datum
     */
    private function writeUnion(Schema $writersSchema, $datum, IOBinaryEncoder $encoder): void
    {
        $datumSchemaIndex = -1;
        $datumSchema = null;
        foreach ($writersSchema->schemas() as $index => $schema) {
            if (Schema::isValidDatum($schema, $datum)) {
                $datumSchemaIndex = $index;
                $datumSchema = $schema;
                break;
            }
        }

        if (null === $datumSchema) {
            throw new IOTypeException($writersSchema, $datum);
        }

        $encoder->writeLong($datumSchemaIndex);
        $this->writeData($datumSchema, $datum, $encoder);
    }

    /**
     * @param Schema|EnumSchema $writersSchema
     * @param mixed             $datum
     */
    private function writeEnum(Schema $writersSchema, $datum, IOBinaryEncoder $encoder): void
    {
        $encoder->writeInt($writersSchema->symbolIndex($datum));
    }

    /**
     * NOTE Unused $writersSchema parameter included for consistency with other write* methods.
     *
     * @param Schema|FixedSchema $writersSchema
     * @param mixed              $datum
     */
    private function writeFixed(Schema $writersSchema, $datum, IOBinaryEncoder $encoder): void
    {
        $encoder->write($datum);
    }

    /**
     * @param Schema|RecordSchema $writersSchema
     * @param mixed               $datum
     */
    private function writeRecord(Schema $writersSchema, $datum, IOBinaryEncoder $encoder): void
    {
        foreach ($writersSchema->fields() as $field) {
            if ($field->hasDefaultValue() && !isset($datum[$field->name()])) {
                $value = $field->defaultValue();
            } else {
                $value = $datum[$field->name()];
            }

            $this->writeData($field->type(), $value, $encoder);
        }
    }
}
