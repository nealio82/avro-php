<?php

namespace Avro\Util;

/**
 * Class for static utility methods used in Avro.
 */
class Util
{
    /**
     * Determines whether the given array is an associative array or a list.
     *
     * @param mixed $array
     */
    public static function isList($array): bool
    {
        if (is_array($array)) {
            $i = 0;
            foreach ($array as $k => $v) {
                if ($i !== $k) {
                    return false;
                }
                ++$i;
            }

            return true;
        }

        return false;
    }

    /**
     * @param mixed $key
     *
     * @return mixed
     */
    public static function arrayValue(array $array, $key)
    {
        return $array[$key] ?? null;
    }
}
