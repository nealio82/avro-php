<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 *  Keeps track of NamedSchema which have been observed so far, as well as the default namespace.
 */
class NamedSchemata
{
    private $schemata;

    /**
     * @param NamedSchemata[] $schemata
     */
    public function __construct(array $schemata = [])
    {
        $this->schemata = $schemata;
    }

    public function hasName(string $fullname): bool
    {
        return array_key_exists($fullname, $this->schemata);
    }

    public function schema(string $fullname): ?Schema
    {
        if (isset($this->schemata[$fullname])) {
            return $this->schemata[$fullname];
        }

        return null;
    }

    public function schemaByName(Name $name): ?Schema
    {
        return $this->schema($name->getFullname());
    }

    /**
     * Creates a new NamedSchemata instance of this schemata instance with the given $schema appended.
     */
    public function cloneWithNewSchema(NamedSchema $schema): self
    {
        $name = $schema->getName()->getFullname();

        if (Schema::isValidType($name)) {
            throw new SchemaParseException(sprintf('Name "%s" is a reserved type name', $name));
        }

        if ($this->hasName($name)) {
            throw new SchemaParseException(sprintf('Name "%s" is already in use', $name));
        }

        $schemata = new self($this->schemata);
        $schemata->schemata[$name] = $schema;

        return $schemata;
    }
}
