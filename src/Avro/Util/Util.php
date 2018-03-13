<?php

namespace Avro\Util;

/**
 * Class for static utility methods used in Avro.
 */
class Util
{
    /**
     * Determines whether the given array is an associative array
     * (what is termed a map, hash, or dictionary in other languages)
     * or a list (an array with monotonically increasing integer indicies
     * starting with zero).
     *
     * @param array $ary array to test
     *
     * @return true if the array is a list and false otherwise
     */
    public static function is_list($ary)
    {
        if (is_array($ary)) {
            $i = 0;
            foreach ($ary as $k => $v) {
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
     * @param array  $ary
     * @param string $key
     *
     * @return mixed the value of $ary[$key] if it is set,
     *               and null otherwise
     */
    public static function array_value($ary, $key)
    {
        return isset($ary[$key]) ? $ary[$key] : null;
    }
}
