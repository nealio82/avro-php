<?php

namespace Avro\Debug;

use Avro\Exception\Exception;

/**
 * Avro library code debugging functions.
 */
class Debug
{
    /**
     * @var int high debug level
     */
    const DEBUG5 = 5;
    /**
     * @var int low debug level
     */
    const DEBUG1 = 1;
    /**
     * @var int current debug level
     */
    const DEBUG_LEVEL = self::DEBUG1;

    /**
     * @var int
     *
     * @param mixed $debug_level
     *
     * @return bool true if the given $debug_level is equivalent
     *              or more verbose than than the current debug level
     *              and false otherwise
     */
    public static function is_debug($debug_level = self::DEBUG1)
    {
        return self::DEBUG_LEVEL >= $debug_level;
    }

    /**
     * @param string $format      format string for the given arguments. Passed as is
     *                            to <code>vprintf</code>.
     * @param array  $args        array of arguments to pass to vsprinf
     * @param int    $debug_level debug level at which to print this statement
     *
     * @return bool true
     */
    public static function debug($format, $args, $debug_level = self::DEBUG1)
    {
        if (self::is_debug($debug_level)) {
            vprintf($format."\n", $args);
        }

        return true;
    }

    /**
     * @param string $str
     *
     * @return string[] array of hex representation of each byte of $str
     */
    public static function hex_array($str)
    {
        return self::bytes_array($str);
    }

    /**
     * @param string $str
     * @param string $joiner string used to join
     *
     * @return string hex-represented bytes of each byte of $str
     *                joined by $joiner
     */
    public static function hex_string($str, $joiner = ' ')
    {
        return implode($joiner, self::hex_array($str));
    }

    /**
     * @param string $str
     * @param string $format format to represent bytes
     *
     * @return string[] array of each byte of $str formatted using $format
     */
    public static function bytes_array($str, $format = 'x%02x')
    {
        $x = [];
        foreach (str_split($str) as $b) {
            $x[] = sprintf($format, ord($b));
        }

        return $x;
    }

    /**
     * @param string $str
     *
     * @return string[] array of bytes of $str represented in decimal format ('%3d')
     */
    public static function dec_array($str)
    {
        return self::bytes_array($str, '%3d');
    }

    /**
     * @param string $str
     * @param string $joiner string to join bytes of $str
     *
     * @return string of bytes of $str represented in decimal format
     *
     * @uses \dec_array()
     */
    public static function dec_string($str, $joiner = ' ')
    {
        return implode($joiner, self::dec_array($str));
    }

    /**
     * @param string $str
     * @param string $format one of 'ctrl', 'hex', or 'dec' for control,
     *                       hexadecimal, or decimal format for bytes.
     *                       - ctrl: ASCII control characters represented as text.
     *                       For example, the null byte is represented as 'NUL'.
     *                       Visible ASCII characters represent themselves, and
     *                       others are represented as a decimal ('%03d')
     *                       - hex: bytes represented in hexadecimal ('%02X')
     *                       - dec: bytes represented in decimal ('%03d')
     *
     * @return string[] array of bytes represented in the given format
     */
    public static function ascii_array($str, $format = 'ctrl')
    {
        if (!in_array($format, ['ctrl', 'hex', 'dec'])) {
            throw new Exception('Unrecognized format specifier');
        }
        $ctrl_chars = ['NUL', 'SOH', 'STX', 'ETX', 'EOT', 'ENQ', 'ACK', 'BEL', 'BS', 'HT', 'LF', 'VT', 'FF', 'CR', 'SO', 'SI', 'DLE', 'DC1', 'DC2', 'DC3', 'DC4', 'NAK', 'SYN', 'ETB', 'CAN', 'EM', 'SUB', 'ESC', 'FS', 'GS', 'RS', 'US'];
        $x = [];
        foreach (str_split($str) as $b) {
            $db = ord($b);
            if ($db < 32) {
                switch ($format) {
                    case 'ctrl':
                        $x[] = str_pad($ctrl_chars[$db], 3, ' ', STR_PAD_LEFT);
                        break;
                    case 'hex':
                        $x[] = sprintf('x%02X', $db);
                        break;
                    case 'dec':
                        $x[] = str_pad($db, 3, '0', STR_PAD_LEFT);
                        break;
                }
            } elseif ($db < 127) {
                $x[] = "  $b";
            } elseif (127 == $db) {
                switch ($format) {
                    case 'ctrl':
                        $x[] = 'DEL';
                        break;
                    case 'hex':
                        $x[] = sprintf('x%02X', $db);
                        break;
                    case 'dec':
                        $x[] = str_pad($db, 3, '0', STR_PAD_LEFT);
                        break;
                }
            } elseif ('hex' == $format) {
                $x[] = sprintf('x%02X', $db);
            } else {
                $x[] = str_pad($db, 3, '0', STR_PAD_LEFT);
            }
        }

        return $x;
    }

    /**
     * @param string $str
     * @param string $format one of 'ctrl', 'hex', or 'dec'.
     *                       See {@link self::ascii_array()} for more description
     * @param string $joiner
     *
     * @return string of bytes joined by $joiner
     *
     * @uses \ascii_array()
     */
    public static function ascii_string($str, $format = 'ctrl', $joiner = ' ')
    {
        return implode($joiner, self::ascii_array($str, $format));
    }
}
