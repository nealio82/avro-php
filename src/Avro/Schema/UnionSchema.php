<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 * Union of Avro schemas, of which values can be of any of the schema in the union.
 */
class UnionSchema extends Schema
{
    /**
     * @var int[] list of indices of named schemas which are defined in
     */
    public $schemaFromSchemataIndices;
    private $schemas;

    /**
     * @param Schema[] $schemas
     */
    public function __construct(array $schemas, ?string $defaultNamespace, NamedSchemata &$schemata = null)
    {
        parent::__construct(Schema::UNION_SCHEMA);

        $this->schemaFromSchemataIndices = [];
        $schemaTypes = [];
        foreach ($schemas as $index => $schema) {
            $isSchemaFromSchemata = false;
            $newSchema = null;
            if (is_string($schema)
                && ($newSchema = $schemata->schemaByName(new Name($schema, null, $defaultNamespace)))
            ) {
                $isSchemaFromSchemata = true;
            } else {
                $newSchema = self::subparse($schema, $defaultNamespace, $schemata);
            }

            $schemaType = $newSchema->type;
            if (self::isValidType($schemaType) && !self::isNamedType($schemaType)
                && in_array($schemaType, $schemaTypes, true)
            ) {
                throw new SchemaParseException(sprintf('"%s" is already in union', $schemaType));
            }

            if (Schema::UNION_SCHEMA === $schemaType) {
                throw new SchemaParseException('Unions cannot contain other unions');
            }

            $schemaTypes[] = $schemaType;
            $this->schemas[] = $newSchema;
            if ($isSchemaFromSchemata) {
                $this->schemaFromSchemataIndices[] = $index;
            }
        }
    }

    /**
     * @return Schema[]
     */
    public function schemas(): array
    {
        return $this->schemas;
    }

    public function schemaByIndex(int $index): Schema
    {
        if (count($this->schemas) <= $index) {
            throw new SchemaParseException('Invalid union schema index');
        }

        return $this->schemas[$index];
    }

    public function toAvro(): array
    {
        $avro = [];

        foreach ($this->schemas as $index => $schema) {
            if (in_array($index, $this->schemaFromSchemataIndices, true)) {
                $avro[] = $schema->getQualifiedName();
            } else {
                $avro[] = $schema->toAvro();
            }
        }

        return $avro;
    }
}
