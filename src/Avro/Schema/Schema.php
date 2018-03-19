<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use Avro\Util\Util;

class Schema
{
    public const NULL_TYPE = 'null';
    public const BOOLEAN_TYPE = 'boolean';

    /**
     * int schema type value is a 32-bit signed int.
     */
    public const INT_TYPE = 'int';

    /**
     * long schema type value is a 64-bit signed int.
     */
    public const LONG_TYPE = 'long';

    /**
     * float schema type value is a 32-bit IEEE 754 floating-point number.
     */
    public const FLOAT_TYPE = 'float';

    /**
     * double schema type value is a 64-bit IEEE 754 floating-point number.
     */
    public const DOUBLE_TYPE = 'double';

    /**
     * string schema type value is a Unicode character sequence.
     */
    public const STRING_TYPE = 'string';

    /**
     * bytes schema type value is a sequence of 8-bit unsigned bytes.
     */
    public const BYTES_TYPE = 'bytes';

    // Complex Types
    // Unnamed Schema
    public const ARRAY_SCHEMA = 'array';
    public const MAP_SCHEMA = 'map';
    public const UNION_SCHEMA = 'union';

    /**
     * Unions of error schemas are used by Avro messages.
     */
    public const ERROR_UNION_SCHEMA = 'error_union';

    // Named Schema
    public const ENUM_SCHEMA = 'enum';
    public const FIXED_SCHEMA = 'fixed';
    public const RECORD_SCHEMA = 'record';

    // Other Schema
    public const ERROR_SCHEMA = 'error';
    public const REQUEST_SCHEMA = 'request';

    // Schema attribute names
    public const TYPE_ATTR = 'type';
    public const NAME_ATTR = 'name';
    public const NAMESPACE_ATTR = 'namespace';
    public const FULLNAME_ATTR = 'fullname';
    public const SIZE_ATTR = 'size';
    public const FIELDS_ATTR = 'fields';
    public const ITEMS_ATTR = 'items';
    public const SYMBOLS_ATTR = 'symbols';
    public const VALUES_ATTR = 'values';
    public const DOC_ATTR = 'doc';
    /**
     * lower bound of integer values: -(1 << 31).
     */
    private const INT_MIN_VALUE = -2147483648;

    /**
     * upper bound of integer values: (1 << 31) - 1.
     */
    private const INT_MAX_VALUE = 2147483647;

    /**
     * lower bound of long values: -(1 << 63).
     */
    private const LONG_MIN_VALUE = -9223372036854775808;

    /**
     * upper bound of long values: (1 << 63) - 1.
     */
    private const LONG_MAX_VALUE = 9223372036854775807;

    private static $primitiveTypes = [
        self::NULL_TYPE,
        self::BOOLEAN_TYPE,
        self::STRING_TYPE,
        self::BYTES_TYPE,
        self::INT_TYPE,
        self::LONG_TYPE,
        self::FLOAT_TYPE,
        self::DOUBLE_TYPE,
    ];

    private static $namedTypes = [
        self::FIXED_SCHEMA,
        self::ENUM_SCHEMA,
        self::RECORD_SCHEMA,
        self::ERROR_SCHEMA,
    ];

    private static $reservedAttrs = [
        self::TYPE_ATTR,
        self::NAME_ATTR,
        self::NAMESPACE_ATTR,
        self::FIELDS_ATTR,
        self::ITEMS_ATTR,
        self::SIZE_ATTR,
        self::SYMBOLS_ATTR,
        self::VALUES_ATTR,
    ];

    /**
     * @internal Should only be called from within the constructor of a class which extends Schema
     *
     * @param string $type a schema type name
     */
    public function __construct($type)
    {
        $this->type = $type;
    }

    public function __toString()
    {
        return json_encode($this->toAvro());
    }

    public static function isNamedType(?string $type): bool
    {
        return in_array($type, self::$namedTypes, true);
    }

    public static function isPrimitiveType(?string $type): bool
    {
        return in_array($type, self::$primitiveTypes, true);
    }

    public static function isValidType(?string $type): bool
    {
        return self::isPrimitiveType($type)
            || self::isNamedType($type)
            || in_array($type, [
                self::ARRAY_SCHEMA,
                self::MAP_SCHEMA,
                self::UNION_SCHEMA,
                self::REQUEST_SCHEMA,
                self::ERROR_UNION_SCHEMA,
            ], true);
    }

    public static function parse(string $json): self
    {
        $schemata = new NamedSchemata();

        return self::realParse(json_decode($json, true), null, $schemata);
    }

    public static function realParse($avro, ?string $defaultNamespace = null, NamedSchemata &$schemata = null): ?self
    {
        if (null === $schemata) {
            $schemata = new NamedSchemata();
        }

        if (is_array($avro)) {
            $type = Util::arrayValue($avro, self::TYPE_ATTR);

            if (self::isPrimitiveType($type)) {
                return new PrimitiveSchema($type);
            }

            if (self::isNamedType($type)) {
                $name = Util::arrayValue($avro, self::NAME_ATTR);
                $namespace = Util::arrayValue($avro, self::NAMESPACE_ATTR);
                $newName = new Name($name, $namespace, $defaultNamespace);
                $doc = Util::arrayValue($avro, self::DOC_ATTR);
                switch ($type) {
                    case self::FIXED_SCHEMA:
                        $size = Util::arrayValue($avro, self::SIZE_ATTR);

                        return new FixedSchema($newName, $doc, $size, $schemata);
                    case self::ENUM_SCHEMA:
                        $symbols = Util::arrayValue($avro, self::SYMBOLS_ATTR);

                        return new EnumSchema($newName, $doc, $symbols, $schemata);
                    case self::RECORD_SCHEMA:
                    case self::ERROR_SCHEMA:
                        $fields = Util::arrayValue($avro, self::FIELDS_ATTR);

                        return new RecordSchema($newName, $doc, $fields, $schemata, $type);
                    default:
                        throw new SchemaParseException(sprintf('Unknown named type: %s', $type));
                }
            } elseif (self::isValidType($type)) {
                switch ($type) {
                    case self::ARRAY_SCHEMA:
                        return new ArraySchema($avro[self::ITEMS_ATTR], $defaultNamespace, $schemata);
                    case self::MAP_SCHEMA:
                        return new MapSchema($avro[self::VALUES_ATTR], $defaultNamespace, $schemata);
                    default:
                        throw new SchemaParseException(sprintf('Unknown valid type: %s', $type));
                }
            } elseif (!array_key_exists(self::TYPE_ATTR, $avro) && Util::isList($avro)) {
                return new UnionSchema($avro, $defaultNamespace, $schemata);
            } else {
                throw new SchemaParseException(sprintf('Undefined type: %s', $type));
            }
        } elseif (self::isPrimitiveType($avro)) {
            return new PrimitiveSchema($avro);
        } else {
            throw new SchemaParseException(sprintf('%s is not a schema we know about.', print_r($avro, true)));
        }
    }

    public static function isValidDatum(self $expectedSchema, $datum): ?bool
    {
        switch ($expectedSchema->type) {
            case self::NULL_TYPE:
                return null === $datum;
            case self::BOOLEAN_TYPE:
                return is_bool($datum);
            case self::STRING_TYPE:
            case self::BYTES_TYPE:
                return is_string($datum);
            case self::INT_TYPE:
                return is_int($datum) && (self::INT_MIN_VALUE <= $datum) && ($datum <= self::INT_MAX_VALUE);
            case self::LONG_TYPE:
                return is_int($datum) && (self::LONG_MIN_VALUE <= $datum) && ($datum <= self::LONG_MAX_VALUE);
            case self::FLOAT_TYPE:
            case self::DOUBLE_TYPE:
                return is_float($datum) || is_int($datum);
            case self::ARRAY_SCHEMA:
                if (is_array($datum)) {
                    foreach ($datum as $d) {
                        if (!self::isValidDatum($expectedSchema->items(), $d)) {
                            return false;
                        }
                    }

                    return true;
                }

                return false;
            case self::MAP_SCHEMA:
                if (is_array($datum)) {
                    foreach ($datum as $k => $v) {
                        if (!is_string($k) || !self::isValidDatum($expectedSchema->values(), $v)) {
                            return false;
                        }
                    }

                    return true;
                }

                return false;
            case self::UNION_SCHEMA:
                foreach ($expectedSchema->schemas() as $schema) {
                    if (self::isValidDatum($schema, $datum)) {
                        return true;
                    }
                }

                return false;
            case self::ENUM_SCHEMA:
                return in_array($datum, $expectedSchema->symbols(), true);
            case self::FIXED_SCHEMA:
                return is_string($datum) && strlen($datum) === $expectedSchema->size();
            case self::RECORD_SCHEMA:
            case self::ERROR_SCHEMA:
            case self::REQUEST_SCHEMA:
                if (is_array($datum)) {
                    foreach ($expectedSchema->fields() as $field) {
                        if ($field->hasDefaultValue() && !isset($datum[$field->name()])) {
                            $value = $field->defaultValue();
                        } else {
                            $value = $datum[$field->name()];
                        }
                    }

                    return !((!$field->hasDefaultValue() && !array_key_exists($field->name(), $datum))
                        || !self::isValidDatum($field->type(), $value));
                }

                return false;
            default:
                throw new SchemaParseException(sprintf('%s is not allowed.', $expectedSchema));
        }
    }

    public function type()
    {
        return $this->type;
    }

    public function toAvro()
    {
        return [self::TYPE_ATTR => $this->type];
    }

    public function attribute(string $attribute): string
    {
        return $this->$attribute();
    }

    protected static function subparse($avro, ?string $defaultNamespace, NamedSchemata &$schemata = null): ?self
    {
        try {
            return self::realParse($avro, $defaultNamespace, $schemata);
        } catch (SchemaParseException $e) {
            throw $e;
        } catch (\Exception $e) {
            throw new SchemaParseException(
                sprintf('Sub-schema is not a valid Avro schema. Bad schema: %s', print_r($avro, true))
            );
        }
    }
}
