<?php

namespace Avro\Datum;

use Avro\Exception\Exception;
use Avro\Exception\IOSchemaMatchException;
use Avro\Schema\Schema;

/**
 * Handles schema-specifc reading of data from the decoder.
 * Also handles schema resolution between the reader and writer schemas (if a writer's schema is provided).
 */
class IODatumReader
{
    private $writersSchema;
    private $readersSchema;

    public function __construct(?Schema $writersSchema = null, ?Schema $readersSchema = null)
    {
        $this->writersSchema = $writersSchema;
        $this->readersSchema = $readersSchema;
    }

    public static function schemasMatch(Schema $writersSchema, Schema $readersSchema): bool
    {
        $writersSchemaType = $writersSchema->type;
        $readersSchemaType = $readersSchema->type;

        if (Schema::UNION_SCHEMA === $writersSchemaType || Schema::UNION_SCHEMA === $readersSchemaType) {
            return true;
        }

        if ($writersSchemaType === $readersSchemaType) {
            if (Schema::isPrimitiveType($writersSchemaType)) {
                return true;
            }

            switch ($readersSchemaType) {
                case Schema::MAP_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema->values(),
                        $readersSchema->values(),
                        [Schema::TYPE_ATTR]
                    );
                case Schema::ARRAY_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema->items(),
                        $readersSchema->items(),
                        [Schema::TYPE_ATTR]
                    );
                case Schema::ENUM_SCHEMA:
                    return self::attributesMatch($writersSchema, $readersSchema, [Schema::FULLNAME_ATTR]);
                case Schema::FIXED_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema,
                        $readersSchema,
                        [Schema::FULLNAME_ATTR, Schema::SIZE_ATTR]
                    );
                case Schema::RECORD_SCHEMA:
                case Schema::ERROR_SCHEMA:
                    return self::attributesMatch($writersSchema, $readersSchema, [Schema::FULLNAME_ATTR]);
                case Schema::REQUEST_SCHEMA:
                    // @todo: This seems wrong
                    return true;
                // @todo: no default
            }

            if (Schema::INT_TYPE === $writersSchemaType
                && in_array($readersSchemaType, [Schema::LONG_TYPE, Schema::FLOAT_TYPE, Schema::DOUBLE_TYPE], true)
            ) {
                return true;
            }

            if (Schema::LONG_TYPE === $writersSchemaType
                && in_array($readersSchemaType, [Schema::FLOAT_TYPE, Schema::DOUBLE_TYPE], true)
            ) {
                return true;
            }

            if (Schema::FLOAT_TYPE === $writersSchemaType && Schema::DOUBLE_TYPE === $readersSchemaType) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks equivalence of the given attributes of the two given schemas.
     *
     * @param string[] $attributeNames
     */
    public static function attributesMatch(Schema $schemaOne, Schema $schemaTwo, array $attributeNames): bool
    {
        foreach ($attributeNames as $attribute_name) {
            if ($schemaOne->attribute($attribute_name) !== $schemaTwo->attribute($attribute_name)) {
                return false;
            }
        }

        return true;
    }

    public function setWritersSchema(Schema $readersSchema): void
    {
        $this->writersSchema = $readersSchema;
    }

    public function read(IOBinaryDecoder $decoder)
    {
        if (null === $this->readersSchema) {
            $this->readersSchema = $this->writersSchema;
        }

        return $this->readData($this->writersSchema, $this->readersSchema, $decoder);
    }

    public function readData(Schema $writersSchema, Schema $readersSchema, IOBinaryDecoder $decoder)
    {
        if (!self::schemasMatch($writersSchema, $readersSchema)) {
            throw new IOSchemaMatchException($writersSchema, $readersSchema);
        }
        // Schema resolution: reader's schema is a union, writer's schema is not
        if (Schema::UNION_SCHEMA === $readersSchema->type() && Schema::UNION_SCHEMA !== $writersSchema->type()) {
            foreach ($readersSchema->schemas() as $schema) {
                if (self::schemasMatch($writersSchema, $schema)) {
                    return $this->readData($writersSchema, $schema, $decoder);
                }
            }

            throw new IOSchemaMatchException($writersSchema, $readersSchema);
        }

        switch ($writersSchema->type()) {
            case Schema::NULL_TYPE:
                return $decoder->readNull();
            case Schema::BOOLEAN_TYPE:
                return $decoder->readBoolean();
            case Schema::INT_TYPE:
                return $decoder->readInt();
            case Schema::LONG_TYPE:
                return $decoder->readLong();
            case Schema::FLOAT_TYPE:
                return $decoder->readFloat();
            case Schema::DOUBLE_TYPE:
                return $decoder->readDouble();
            case Schema::STRING_TYPE:
                return $decoder->readString();
            case Schema::BYTES_TYPE:
                return $decoder->readBytes();
            case Schema::ARRAY_SCHEMA:
                return $this->readArray($writersSchema, $readersSchema, $decoder);
            case Schema::MAP_SCHEMA:
                return $this->readMap($writersSchema, $readersSchema, $decoder);
            case Schema::UNION_SCHEMA:
                return $this->readUnion($writersSchema, $readersSchema, $decoder);
            case Schema::ENUM_SCHEMA:
                return $this->readEnum($writersSchema, $readersSchema, $decoder);
            case Schema::FIXED_SCHEMA:
                return $this->readFixed($writersSchema, $readersSchema, $decoder);
            case Schema::RECORD_SCHEMA:
            case Schema::ERROR_SCHEMA:
            case Schema::REQUEST_SCHEMA:
                return $this->readRecord($writersSchema, $readersSchema, $decoder);
            default:
                throw new Exception(sprintf('Cannot read unknown schema type: %s', $writersSchema->type()));
        }
    }

    public function readArray(Schema $writersSchema, Schema $readersSchema, IOBinaryDecoder $decoder): array
    {
        $items = [];
        $blockCount = $decoder->readLong();
        while (0 !== $blockCount) {
            if ($blockCount < 0) {
                $blockCount = -$blockCount;
                $blockSize = $decoder->readLong(); // Read (and ignore) block size
            }
            for ($i = 0; $i < $blockCount; ++$i) {
                $items[] = $this->readData($writersSchema->items(), $readersSchema->items(), $decoder);
            }
            $blockCount = $decoder->readLong();
        }

        return $items;
    }

    public function readMap(Schema $writersSchema, Schema $readersSchema, IOBinaryDecoder $decoder): array
    {
        $items = [];
        $pairCount = $decoder->readLong();
        while (0 !== $pairCount) {
            if ($pairCount < 0) {
                $pairCount = -$pairCount;
                $blockSize = $decoder->readLong(); // Read (and ignore) block size
            }

            for ($i = 0; $i < $pairCount; ++$i) {
                $key = $decoder->readString();

                $items[$key] = $this->readData($writersSchema->values(), $readersSchema->values(), $decoder);
            }
            $pairCount = $decoder->readLong();
        }

        return $items;
    }

    public function readUnion(Schema $writersSchema, Schema $readersSchema, IOBinaryDecoder $decoder)
    {
        $selectedWritersSchema = $writersSchema->schemaByIndex($decoder->readLong());

        return $this->readData($selectedWritersSchema, $readersSchema, $decoder);
    }

    public function readEnum(Schema $writersSchema, Schema $readersSchema, IOBinaryDecoder $decoder)
    {
        $symbolIndex = $decoder->readInt();
        $symbol = $writersSchema->symbolByIndex($symbolIndex);

        if (!$readersSchema->hasSymbol($symbol)) {
            null;
        } // @todo: unset write schema resolution

        return $symbol;
    }

    public function readFixed(Schema $writersSchema, Schema $readersSchema, IOBinaryDecoder $decoder): string
    {
        return $decoder->read($writersSchema->size());
    }

    public function readRecord(Schema $writersSchema, Schema $readersSchema, IOBinaryDecoder $decoder): array
    {
        $readersFields = $readersSchema->fieldsHash();
        $record = [];
        foreach ($writersSchema->fields() as $writersField) {
            $type = $writersField->type();
            if (isset($readersFields[$writersField->name()])) {
                $record[$writersField->name()] = $this->readData(
                    $type,
                    $readersFields[$writersField->name()]->type(),
                    $decoder
                );
            } else {
                $this->skipData($type, $decoder);
            }
        }

        // Fill in default values
        if (count($readersFields) > count($record)) {
            $writers_fields = $writersSchema->fieldsHash();
            foreach ($readersFields as $fieldName => $field) {
                if (!isset($writers_fields[$fieldName])) {
                    if ($field->hasDefaultValue()) {
                        $record[$field->name()] = $this->readDefaultValue($field->type(), $field->defaultValue());
                    } else {
                        null;
                    } // @todo: unset
                }
            }
        }

        return $record;
    }

    public function readDefaultValue(Schema $fieldSchema, $defaultValue)
    {
        switch ($fieldSchema->type()) {
            case Schema::NULL_TYPE:
                return null;
            case Schema::BOOLEAN_TYPE:
                return $defaultValue;
            case Schema::INT_TYPE:
            case Schema::LONG_TYPE:
                return (int) $defaultValue;
            case Schema::FLOAT_TYPE:
            case Schema::DOUBLE_TYPE:
                return (float) $defaultValue;
            case Schema::STRING_TYPE:
            case Schema::BYTES_TYPE:
                return $defaultValue;
            case Schema::ARRAY_SCHEMA:
                $array = [];
                foreach ($defaultValue as $jsonValue) {
                    $val = $this->readDefaultValue($fieldSchema->items(), $jsonValue);
                    $array[] = $val;
                }

                return $array;
            case Schema::MAP_SCHEMA:
                $map = [];
                foreach ($defaultValue as $key => $jsonValue) {
                    $map[$key] = $this->readDefaultValue($fieldSchema->values(), $jsonValue);
                }

                return $map;
            case Schema::UNION_SCHEMA:
                return $this->readDefaultValue($fieldSchema->schemaByIndex(0), $defaultValue);
            case Schema::ENUM_SCHEMA:
            case Schema::FIXED_SCHEMA:
                return $defaultValue;
            case Schema::RECORD_SCHEMA:
                $record = [];
                foreach ($fieldSchema->fields() as $field) {
                    $fieldName = $field->name();
                    if (!$jsonValue = $defaultValue[$fieldName]) {
                        $jsonValue = $field->defaultValue();
                    }

                    $record[$fieldName] = $this->readDefaultValue($field->type(), $jsonValue);
                }

                return $record;
            default:
                throw new Exception(sprintf('Unknown type: %s', $fieldSchema->type()));
        }
    }

    private function skipData(Schema $writersSchema, IOBinaryDecoder $decoder): void
    {
        switch ($writersSchema->type()) {
            case Schema::NULL_TYPE:
                $decoder->skipNull();
                break;
            case Schema::BOOLEAN_TYPE:
                $decoder->skipBoolean();
                break;
            case Schema::INT_TYPE:
                $decoder->skipInt();
                break;
            case Schema::LONG_TYPE:
                $decoder->skipLong();
                break;
            case Schema::FLOAT_TYPE:
                $decoder->skipFloat();
                break;
            case Schema::DOUBLE_TYPE:
                $decoder->skipDouble();
                break;
            case Schema::STRING_TYPE:
                $decoder->skipString();
                break;
            case Schema::BYTES_TYPE:
                $decoder->skipBytes();
                break;
            case Schema::ARRAY_SCHEMA:
                $decoder->skipArray($writersSchema, $decoder);
                break;
            case Schema::MAP_SCHEMA:
                $decoder->skipMap($writersSchema, $decoder);
                break;
            case Schema::UNION_SCHEMA:
                $decoder->skipUnion($writersSchema, $decoder);
                break;
            case Schema::ENUM_SCHEMA:
                $decoder->skipEnum($writersSchema, $decoder);
                break;
            case Schema::FIXED_SCHEMA:
                $decoder->skipFixed($writersSchema, $decoder);
                break;
            case Schema::RECORD_SCHEMA:
            case Schema::ERROR_SCHEMA:
            case Schema::REQUEST_SCHEMA:
                $decoder->skipRecord($writersSchema, $decoder);
                break;
            default:
                throw new Exception(sprintf('Uknown schema type: %s', $writersSchema->type()));
        }
    }
}
