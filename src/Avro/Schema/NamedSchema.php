<?php

namespace Avro\Schema;

/**
 * Parent class of named Avro schema.
 *
 * @todo Refactor NamedSchema to use an Name instance to store name information.
 */
class NamedSchema extends Schema
{
    private $name;
    private $doc;

    public function __construct(string $type, Name $name, ?string $doc = null, NamedSchemata &$schemata = null)
    {
        parent::__construct($type);

        $this->name = $name;
        $this->doc = $doc;

        if (null !== $schemata) {
            $schemata = $schemata->cloneWithNewSchema($this);
        }
    }

    public function getName(): Name
    {
        return $this->name;
    }

    public function toAvro()
    {
        $avro = parent::toAvro();
        $avro[Schema::NAME_ATTR] = Name::extractName($this->name->getQualifiedName());

        $namespace = Name::extractNamespace($this->name->getQualifiedName());
        if ($namespace) {
            $avro[Schema::NAMESPACE_ATTR] = $namespace;
        }

        if (null !== $this->doc) {
            $avro[Schema::DOC_ATTR] = $this->doc;
        }

        return $avro;
    }

    protected function fullname(): string
    {
        return $this->name->getFullname();
    }
}
