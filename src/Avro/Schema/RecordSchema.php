<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use Avro\Util\Util;

class RecordSchema extends NamedSchema
{
    /**
     * @var Schema[]
     */
    private $fields;

    /**
     * @var array map of field names to field objects
     *
     * @internal Not called directly. Memoization of RecordSchema->fieldsHash()
     */
    private $fieldsHash;

    public function __construct(
        Name $name,
        ?string $doc,
        array $fields,
        NamedSchemata &$schemata = null,
        string $schemaType = Schema::RECORD_SCHEMA
    ) {
        if (null === $fields) {
            throw new SchemaParseException(
                'Record schema requires a non-empty fields attribute');
        }

        if (Schema::REQUEST_SCHEMA === $schemaType) {
            parent::__construct($schemaType, $name);
        } else {
            parent::__construct($schemaType, $name, $doc, $schemata);
        }

        $this->fields = self::parseFields($fields, $name->getNamespace(), $schemata);
    }

    public static function parseFields(array $fieldData, ?string $defaultNamespace, NamedSchemata &$schemata): array
    {
        $fields = [];
        $fieldNames = [];
        foreach ($fieldData as $index => $field) {
            $name = Util::arrayValue($field, Field::FIELD_NAME_ATTR);
            $type = Util::arrayValue($field, Schema::TYPE_ATTR);
            $order = Util::arrayValue($field, Field::ORDER_ATTR);

            $default = null;
            $hasDefault = false;
            if (array_key_exists(Field::DEFAULT_ATTR, $field)) {
                $default = $field[Field::DEFAULT_ATTR];
                $hasDefault = true;
            }

            if (in_array($name, $fieldNames, true)) {
                throw new SchemaParseException(
                    sprintf('Field name %s is already in use', $name));
            }

            $isSchemaFromSchemata = false;
            $fieldSchema = null;
            if (is_string($type) && $fieldSchema = $schemata->schemaByName(new Name($type, null, $defaultNamespace))) {
                $isSchemaFromSchemata = true;
            } else {
                $fieldSchema = self::subparse($type, $defaultNamespace, $schemata);
            }

            $newField = new Field($name, $fieldSchema, $isSchemaFromSchemata, $hasDefault, $default, $order);
            $fieldNames[] = $name;
            $fields[] = $newField;
        }

        return $fields;
    }

    public function toAvro(): array
    {
        $avro = parent::toAvro();

        $fieldsAvro = [];
        foreach ($this->fields as $field) {
            $fieldsAvro[] = $field->toAvro();
        }

        if (Schema::REQUEST_SCHEMA === $this->type) {
            return $fieldsAvro;
        }

        $avro[Schema::FIELDS_ATTR] = $fieldsAvro;

        return $avro;
    }

    /**
     * @return array the schema definitions of the fields of this RecordSchema
     */
    public function fields(): array
    {
        return $this->fields;
    }

    /**
     * @return array a hash table of the fields of this RecordSchema fields keyed by each field's name
     */
    public function fieldsHash(): array
    {
        if (null === $this->fieldsHash) {
            $hash = [];
            foreach ($this->fields as $field) {
                $hash[$field->name()] = $field;
            }
            $this->fieldsHash = $hash;
        }

        return $this->fieldsHash;
    }
}
