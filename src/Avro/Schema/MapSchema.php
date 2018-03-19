<?php

namespace Avro\Schema;

/**
 * Avro map schema consisting of named values of defined Avro Schema types.
 */
class MapSchema extends Schema
{
    private $values;

    /**
     * @todo: Couldn't we derive this based on whether or not $this->values is a string?
     */
    private $isValuesSchemaFromSchemata;

    /**
     * @param mixed $values
     */
    public function __construct($values, ?string $defaultNamespace, NamedSchemata &$schemata = null)
    {
        parent::__construct(Schema::MAP_SCHEMA);

        $this->isValuesSchemaFromSchemata = false;
        $valuesSchema = null;
        if (is_string($values)
            && $valuesSchema = $schemata->schemaByName(new Name($values, null, $defaultNamespace))
        ) {
            $this->isValuesSchemaFromSchemata = true;
        } else {
            $valuesSchema = Schema::subparse($values, $defaultNamespace, $schemata);
        }

        $this->values = $valuesSchema;
    }

    public function values(): ?Schema
    {
        return $this->values;
    }

    public function toAvro(): array
    {
        $avro = parent::toAvro();
        $avro[Schema::VALUES_ATTR] = $this->isValuesSchemaFromSchemata
            ? $this->values->getQualifiedName() : $this->values->toAvro();

        return $avro;
    }
}
