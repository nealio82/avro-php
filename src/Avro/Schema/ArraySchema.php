<?php

namespace Avro\Schema;

/**
 * Avro array schema, consisting of items of a particular Avro schema type.
 */
class ArraySchema extends Schema
{
    /**
     * @var Name|Schema named schema name or Schema of array element
     */
    private $items;

    /**
     * @todo: couldn't we derive this from whether or not $this->items is an Name or an Schema?
     */
    private $isItemsSchemaFromSchemata;

    /**
     * @param mixed $items
     */
    public function __construct($items, ?string $defaultNamespace, ?NamedSchemata &$schemata = null)
    {
        parent::__construct(Schema::ARRAY_SCHEMA);

        $this->isItemsSchemaFromSchemata = false;
        $itemsSchema = null;
        if (is_string($items) && $itemsSchema = $schemata->schemaByName(new Name($items, null, $defaultNamespace))) {
            $this->isItemsSchemaFromSchemata = true;
        } else {
            $itemsSchema = Schema::subparse($items, $defaultNamespace, $schemata);
        }

        $this->items = $itemsSchema;
    }

    /**
     * @return Name|Schema named schema name or Schema of this array schema's elements
     */
    public function items()
    {
        return $this->items;
    }

    public function toAvro(): array
    {
        $avro = parent::toAvro();
        $avro[Schema::ITEMS_ATTR] = $this->isItemsSchemaFromSchemata
            ? $this->items->getQualifiedName() : $this->items->toAvro();

        return $avro;
    }
}
