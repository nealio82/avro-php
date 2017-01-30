<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use Avro\Util\Util;

class Schema
{
    /**
     * @var int lower bound of integer values: -(1 << 31)
     */
    const INT_MIN_VALUE = -2147483648;

    /**
     * @var int upper bound of integer values: (1 << 31) - 1
     */
    const INT_MAX_VALUE = 2147483647;

    /**
     * @var long lower bound of long values: -(1 << 63)
     */
    const LONG_MIN_VALUE = -9223372036854775808;

    /**
     * @var long upper bound of long values: (1 << 63) - 1
     */
    const LONG_MAX_VALUE =  9223372036854775807;

    /**
     * @var string null schema type name
     */
    const NULL_TYPE = 'null';

    /**
     * @var string boolean schema type name
     */
    const BOOLEAN_TYPE = 'boolean';

    /**
     * int schema type value is a 32-bit signed int
     * @var string int schema type name.
     */
    const INT_TYPE = 'int';

    /**
     * long schema type value is a 64-bit signed int
     * @var string long schema type name
     */
    const LONG_TYPE = 'long';

    /**
     * float schema type value is a 32-bit IEEE 754 floating-point number
     * @var string float schema type name
     */
    const FLOAT_TYPE = 'float';

    /**
     * double schema type value is a 64-bit IEEE 754 floating-point number
     * @var string double schema type name
     */
    const DOUBLE_TYPE = 'double';

    /**
     * string schema type value is a Unicode character sequence
     * @var string string schema type name
     */
    const STRING_TYPE = 'string';

    /**
     * bytes schema type value is a sequence of 8-bit unsigned bytes
     * @var string bytes schema type name
     */
    const BYTES_TYPE = 'bytes';

    // Complex Types
    // Unnamed Schema
    /**
     * @var string array schema type name
     */
    const ARRAY_SCHEMA = 'array';

    /**
     * @var string map schema type name
     */
    const MAP_SCHEMA = 'map';

    /**
     * @var string union schema type name
     */
    const UNION_SCHEMA = 'union';

    /**
     * Unions of error schemas are used by Avro messages
     * @var string error_union schema type name
     */
    const ERROR_UNION_SCHEMA = 'error_union';

    // Named Schema

    /**
     * @var string enum schema type name
     */
    const ENUM_SCHEMA = 'enum';

    /**
     * @var string fixed schema type name
     */
    const FIXED_SCHEMA = 'fixed';

    /**
     * @var string record schema type name
     */
    const RECORD_SCHEMA = 'record';
    // Other Schema

    /**
     * @var string error schema type name
     */
    const ERROR_SCHEMA = 'error';

    /**
     * @var string request schema type name
     */
    const REQUEST_SCHEMA = 'request';


    // Schema attribute names
    /**
     * @var string schema type name attribute name
     */
    const TYPE_ATTR = 'type';

    /**
     * @var string named schema name attribute name
     */
    const NAME_ATTR = 'name';

    /**
     * @var string named schema namespace attribute name
     */
    const NAMESPACE_ATTR = 'namespace';

    /**
     * @var string derived attribute: doesn't appear in schema
     */
    const FULLNAME_ATTR = 'fullname';

    /**
     * @var string array schema size attribute name
     */
    const SIZE_ATTR = 'size';

    /**
     * @var string record fields attribute name
     */
    const FIELDS_ATTR = 'fields';

    /**
     * @var string array schema items attribute name
     */
    const ITEMS_ATTR = 'items';

    /**
     * @var string enum schema symbols attribute name
     */
    const SYMBOLS_ATTR = 'symbols';

    /**
     * @var string map schema values attribute name
     */
    const VALUES_ATTR = 'values';

    /**
     * @var string document string attribute name
     */
    const DOC_ATTR = 'doc';

    /**
     * @var array list of primitive schema type names
     */
    private static $primitive_types = array(self::NULL_TYPE, self::BOOLEAN_TYPE,
        self::STRING_TYPE, self::BYTES_TYPE,
        self::INT_TYPE, self::LONG_TYPE,
        self::FLOAT_TYPE, self::DOUBLE_TYPE);

    /**
     * @var array list of named schema type names
     */
    private static $named_types = array(self::FIXED_SCHEMA, self::ENUM_SCHEMA,
        self::RECORD_SCHEMA, self::ERROR_SCHEMA);

    /**
     * @param string $type a schema type name
     * @returns boolean true if the given type name is a named schema type name
     *                  and false otherwise.
     */
    public static function is_named_type($type)
    {
        return in_array($type, self::$named_types);
    }

    /**
     * @param string $type a schema type name
     * @returns boolean true if the given type name is a primitive schema type
     *                  name and false otherwise.
     */
    public static function is_primitive_type($type)
    {
        return in_array($type, self::$primitive_types);
    }

    /**
     * @param string $type a schema type name
     * @returns boolean true if the given type name is a valid schema type
     *                  name and false otherwise.
     */
    public static function is_valid_type($type)
    {
        return (self::is_primitive_type($type)
            || self::is_named_type($type)
            || in_array($type, array(self::ARRAY_SCHEMA,
                self::MAP_SCHEMA,
                self::UNION_SCHEMA,
                self::REQUEST_SCHEMA,
                self::ERROR_UNION_SCHEMA)));
    }

    /**
     * @var array list of names of reserved attributes
     */
    private static $reserved_attrs = array(self::TYPE_ATTR,
        self::NAME_ATTR,
        self::NAMESPACE_ATTR,
        self::FIELDS_ATTR,
        self::ITEMS_ATTR,
        self::SIZE_ATTR,
        self::SYMBOLS_ATTR,
        self::VALUES_ATTR);

    /**
     * @param string $json JSON-encoded schema
     * @uses self::real_parse()
     * @returns Schema
     */
    public static function parse($json)
    {
        $schemata = new NamedSchemata();
        return self::real_parse(json_decode($json, true), null, $schemata);
    }

    /**
     * @param mixed $avro JSON-decoded schema
     * @param string $default_namespace namespace of enclosing schema
     * @param NamedSchemata &$schemata reference to named schemas
     * @returns Schema
     * @throws SchemaParseException
     */
    static function real_parse($avro, $default_namespace=null, NamedSchemata &$schemata=null)
    {
        if (is_null($schemata))
            $schemata = new NamedSchemata();

        if (is_array($avro))
        {
            $type = Util::array_value($avro, self::TYPE_ATTR);

            if (self::is_primitive_type($type))
                return new PrimitiveSchema($type);

            elseif (self::is_named_type($type))
            {
                $name = Util::array_value($avro, self::NAME_ATTR);
                $namespace = Util::array_value($avro, self::NAMESPACE_ATTR);
                $new_name = new Name($name, $namespace, $default_namespace);
                $doc = Util::array_value($avro, self::DOC_ATTR);
                switch ($type)
                {
                    case self::FIXED_SCHEMA:
                        $size = Util::array_value($avro, self::SIZE_ATTR);
                        return new FixedSchema($new_name, $doc,
                            $size,
                            $schemata);
                    case self::ENUM_SCHEMA:
                        $symbols = Util::array_value($avro, self::SYMBOLS_ATTR);
                        return new EnumSchema($new_name, $doc,
                            $symbols,
                            $schemata);
                    case self::RECORD_SCHEMA:
                    case self::ERROR_SCHEMA:
                        $fields = Util::array_value($avro, self::FIELDS_ATTR);
                        return new RecordSchema($new_name, $doc,
                            $fields,
                            $schemata, $type);
                    default:
                        throw new SchemaParseException(
                            sprintf('Unknown named type: %s', $type));
                }
            }
            elseif (self::is_valid_type($type))
            {
                switch ($type)
                {
                    case self::ARRAY_SCHEMA:
                        return new ArraySchema($avro[self::ITEMS_ATTR],
                            $default_namespace,
                            $schemata);
                    case self::MAP_SCHEMA:
                        return new MapSchema($avro[self::VALUES_ATTR],
                            $default_namespace,
                            $schemata);
                    default:
                        throw new SchemaParseException(
                            sprintf('Unknown valid type: %s', $type));
                }
            }
            elseif (!array_key_exists(self::TYPE_ATTR, $avro)
                && Util::is_list($avro))
                return new UnionSchema($avro, $default_namespace, $schemata);
            else
                throw new SchemaParseException(sprintf('Undefined type: %s',
                    $type));
        }
        elseif (self::is_primitive_type($avro))
            return new PrimitiveSchema($avro);
        else
            throw new SchemaParseException(
                sprintf('%s is not a schema we know about.',
                    print_r($avro, true)));
    }

    /**
     * @returns boolean true if $datum is valid for $expected_schema
     *                  and false otherwise.
     * @throws SchemaParseException
     */
    public static function is_valid_datum($expected_schema, $datum)
    {
        switch($expected_schema->type)
        {
            case self::NULL_TYPE:
                return is_null($datum);
            case self::BOOLEAN_TYPE:
                return is_bool($datum);
            case self::STRING_TYPE:
            case self::BYTES_TYPE:
                return is_string($datum);
            case self::INT_TYPE:
                return (is_int($datum)
                    && (self::INT_MIN_VALUE <= $datum)
                    && ($datum <= self::INT_MAX_VALUE));
            case self::LONG_TYPE:
                return (is_int($datum)
                    && (self::LONG_MIN_VALUE <= $datum)
                    && ($datum <= self::LONG_MAX_VALUE));
            case self::FLOAT_TYPE:
            case self::DOUBLE_TYPE:
                return (is_float($datum) || is_int($datum));
            case self::ARRAY_SCHEMA:
                if (is_array($datum))
                {
                    foreach ($datum as $d)
                        if (!self::is_valid_datum($expected_schema->items(), $d))
                            return false;
                    return true;
                }
                return false;
            case self::MAP_SCHEMA:
                if (is_array($datum))
                {
                    foreach ($datum as $k => $v)
                        if (!is_string($k)
                            || !self::is_valid_datum($expected_schema->values(), $v))
                            return false;
                    return true;
                }
                return false;
            case self::UNION_SCHEMA:
                foreach ($expected_schema->schemas() as $schema)
                    if (self::is_valid_datum($schema, $datum))
                        return true;
                return false;
            case self::ENUM_SCHEMA:
                return in_array($datum, $expected_schema->symbols());
            case self::FIXED_SCHEMA:
                return (is_string($datum)
                    && (strlen($datum) == $expected_schema->size()));
            case self::RECORD_SCHEMA:
            case self::ERROR_SCHEMA:
            case self::REQUEST_SCHEMA:
                if (is_array($datum))
                {
                    foreach ($expected_schema->fields() as $field)
                        if (!array_key_exists($field->name(), $datum) || !self::is_valid_datum($field->type(), $datum[$field->name()]))
                            return false;
                    return true;
                }
                return false;
            default:
                throw new SchemaParseException(
                    sprintf('%s is not allowed.', $expected_schema));
        }
    }

    /**
     * @internal Should only be called from within the constructor of
     *           a class which extends Schema
     * @param string $type a schema type name
     */
    public function __construct($type)
    {
        $this->type = $type;
    }

    /**
     * @param mixed $avro
     * @param string $default_namespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     * @returns Schema
     * @uses Schema::real_parse()
     * @throws SchemaParseException
     */
    protected static function subparse($avro, $default_namespace, NamedSchemata &$schemata=null)
    {
        try
        {
            return self::real_parse($avro, $default_namespace, $schemata);
        }
        catch (SchemaParseException $e)
        {
            throw $e;
        }
        catch (Exception $e)
        {
            throw new SchemaParseException(
                sprintf('Sub-schema is not a valid Avro schema. Bad schema: %s',
                    print_r($avro, true)));
        }

    }

    /**
     * @returns string schema type name of this schema
     */
    public function type() { return $this->type;  }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        return array(self::TYPE_ATTR => $this->type);
    }

    /**
     * @returns string the JSON-encoded representation of this Avro schema.
     */
    public function __toString() { return json_encode($this->to_avro()); }

    /**
     * @returns mixed value of the attribute with the given attribute name
     */
    public function attribute($attribute) { return $this->$attribute(); }

}