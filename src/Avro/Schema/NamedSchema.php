<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 * Parent class of named Avro schema.
 *
 * @todo Refactor NamedSchema to use an AvroName instance
 *       to store name information.
 */
class NamedSchema extends Schema
{
    /**
     * @var Name
     */
    private $name;

    /**
     * @var string documentation string
     */
    private $doc;

    /**
     * @param string        $type
     * @param Name          $name
     * @param string        $doc       documentation string
     * @param NamedSchemata &$schemata
     *
     * @throws SchemaParseException
     */
    public function __construct($type, Name $name, $doc = null, NamedSchemata &$schemata = null)
    {
        parent::__construct($type);
        $this->name = $name;

        if ($doc && !is_string($doc)) {
            throw new SchemaParseException('Schema doc attribute must be a string');
        }
        $this->doc = $doc;

        if (null !== $schemata) {
            $schemata = $schemata->clone_with_new_schema($this);
        }
    }

    /**
     * @return mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();
        [$name, $namespace] = Name::extract_namespace($this->qualified_name());
        $avro[Schema::NAME_ATTR] = $name;
        if ($namespace) {
            $avro[Schema::NAMESPACE_ATTR] = $namespace;
        }
        if (null !== $this->doc) {
            $avro[Schema::DOC_ATTR] = $this->doc;
        }

        return $avro;
    }

    /**
     * @return string
     */
    public function fullname()
    {
        return $this->name->fullname();
    }

    public function qualified_name()
    {
        return $this->name->qualified_name();
    }
}
