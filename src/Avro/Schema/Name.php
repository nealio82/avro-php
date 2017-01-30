<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 * @package Avro
 */
class Name
{
    /**
     * @var string character used to separate names comprising the fullname
     */
    const NAME_SEPARATOR = '.';

    /**
     * @var string regular expression to validate name values
     */
    const NAME_REGEXP = '/^[A-Za-z_][A-Za-z0-9_]*$/';

    /**
     * @returns string[] array($name, $namespace)
     */
    public static function extract_namespace($name, $namespace = null)
    {
        $parts = explode(self::NAME_SEPARATOR, $name);
        if (count($parts) > 1) {
            $name = array_pop($parts);
            $namespace = join(self::NAME_SEPARATOR, $parts);
        }
        return array($name, $namespace);
    }

    /**
     * @returns boolean true if the given name is well-formed
     *          (is a non-null, non-empty string) and false otherwise
     */
    public static function is_well_formed_name($name)
    {
        return (is_string($name) && !empty($name)
            && preg_match(self::NAME_REGEXP, $name));
    }

    /**
     * @param string $namespace
     * @returns boolean true if namespace is composed of valid names
     * @throws SchemaParseException if any of the namespace components
     *                                  are invalid.
     */
    private static function check_namespace_names($namespace)
    {
        foreach (explode(self::NAME_SEPARATOR, $namespace) as $n) {
            if (empty($n) || (0 == preg_match(self::NAME_REGEXP, $n)))
                throw new SchemaParseException(sprintf('Invalid name "%s"', $n));
        }
        return true;
    }

    /**
     * @param string $name
     * @param string $namespace
     * @returns string
     * @throws SchemaParseException if any of the names are not valid.
     */
    private static function parse_fullname($name, $namespace)
    {
        if (!is_string($namespace) || empty($namespace))
            throw new SchemaParseException('Namespace must be a non-empty string.');
        self::check_namespace_names($namespace);
        return $namespace . '.' . $name;
    }

    /**
     * @var string valid names are matched by self::NAME_REGEXP
     */
    private $name;

    /**
     * @var string
     */
    private $namespace;

    /**
     * @var string
     */
    private $fullname;

    /**
     * @var string Name qualified as necessary given its default namespace.
     */
    private $qualified_name;

    /**
     * @param string $name
     * @param string $namespace
     * @param string $default_namespace
     */
    public function __construct($name, $namespace, $default_namespace)
    {
        if (!is_string($name) || empty($name)) {
            throw new SchemaParseException('Name must be a non-empty string.');
        }

        if (strpos($name, self::NAME_SEPARATOR)
            && self::check_namespace_names($name)
        ) {
            $this->fullname = $name;
        } elseif (0 == preg_match(self::NAME_REGEXP, $name)) {
            throw new SchemaParseException(sprintf('Invalid name "%s"', $name));
        } elseif (!is_null($namespace)) {
            $this->fullname = self::parse_fullname($name, $namespace);
        } elseif (!is_null($default_namespace)) {
            $this->fullname = self::parse_fullname($name, $default_namespace);
        } else {
            $this->fullname = $name;
        }

        list($this->name, $this->namespace) = self::extract_namespace($this->fullname);
        $this->qualified_name = (is_null($this->namespace)
            || $this->namespace == $default_namespace)
            ? $this->name : $this->fullname;
    }

    /**
     * @returns array array($name, $namespace)
     */
    public function name_and_namespace()
    {
        return array($this->name, $this->namespace);
    }

    /**
     * @returns string
     */
    public function fullname()
    {
        return $this->fullname;
    }

    /**
     * @returns string fullname
     * @uses $this->fullname()
     */
    public function __toString()
    {
        return $this->fullname();
    }

    /**
     * @returns string name qualified for its context
     */
    public function qualified_name()
    {
        return $this->qualified_name;
    }

}