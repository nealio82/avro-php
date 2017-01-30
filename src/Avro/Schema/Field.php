<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

/**
 * Field of an {@link RecordSchema}
 * @package Avro
 */
class Field extends Schema
{

    /**
     * @var string fields name attribute name
     */
    const FIELD_NAME_ATTR = 'name';

    /**
     * @var string
     */
    const DEFAULT_ATTR = 'default';

    /**
     * @var string
     */
    const ORDER_ATTR = 'order';

    /**
     * @var string
     */
    const ASC_SORT_ORDER = 'ascending';

    /**
     * @var string
     */
    const DESC_SORT_ORDER = 'descending';

    /**
     * @var string
     */
    const IGNORE_SORT_ORDER = 'ignore';

    /**
     * @var array list of valid field sort order values
     */
    private static $valid_field_sort_orders = array(self::ASC_SORT_ORDER,
        self::DESC_SORT_ORDER,
        self::IGNORE_SORT_ORDER);


    /**
     * @param string $order
     * @returns boolean
     */
    private static function is_valid_field_sort_order($order)
    {
        return in_array($order, self::$valid_field_sort_orders);
    }

    /**
     * @param string $order
     * @throws SchemaParseException if $order is not a valid
     *                                  field order value.
     */
    private static function check_order_value($order)
    {
        if (!is_null($order) && !self::is_valid_field_sort_order($order))
            throw new SchemaParseException(
                sprintf('Invalid field sort order %s', $order));
    }

    /**
     * @var string
     */
    private $name;

    /**
     * @var boolean whether or no there is a default value
     */
    private $has_default;

    /**
     * @var string field default value
     */
    private $default;

    /**
     * @var string sort order of this field
     */
    private $order;

    /**
     * @var boolean whether or not the NamedSchema of this field is
     *              defined in the NamedSchemata instance
     */
    private $is_type_from_schemata;

    /**
     * @param string $type
     * @param string $name
     * @param Schema $schema
     * @param boolean $is_type_from_schemata
     * @param string $default
     * @param string $order
     * @todo Check validity of $default value
     * @todo Check validity of $order value
     */
    public function __construct($name, Schema $schema, $is_type_from_schemata,
                                $has_default, $default, $order = null)
    {
        if (!Name::is_well_formed_name($name))
            throw new SchemaParseException('Field requires a "name" attribute');

        $this->type = $schema;
        $this->is_type_from_schemata = $is_type_from_schemata;
        $this->name = $name;
        $this->has_default = $has_default;
        if ($this->has_default)
            $this->default = $default;
        $this->check_order_value($order);
        $this->order = $order;
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = array(Field::FIELD_NAME_ATTR => $this->name);

        $avro[Schema::TYPE_ATTR] = ($this->is_type_from_schemata)
            ? $this->type->qualified_name() : $this->type->to_avro();

        if (isset($this->default))
            $avro[Field::DEFAULT_ATTR] = $this->default;

        if ($this->order)
            $avro[Field::ORDER_ATTR] = $this->order;

        return $avro;
    }

    /**
     * @returns string the name of this field
     */
    public function name()
    {
        return $this->name;
    }

    /**
     * @returns mixed the default value of this field
     */
    public function default_value()
    {
        return $this->default;
    }

    /**
     * @returns boolean true if the field has a default and false otherwise
     */
    public function has_default_value()
    {
        return $this->has_default;
    }
}