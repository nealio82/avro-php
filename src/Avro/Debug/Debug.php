<?php

namespace Avro\Debug;

use Avro\Exception\Exception;

/**
 * Avro library code debugging functions.
 */
class Debug
{
    private const DEBUG_LEVEL_LOW = 1;
    private const DEBUG_LEVEL_HIGH = 5;

    public static $debugLevel = self::DEBUG_LEVEL_LOW;

    /**
     * @param string $format     format string for the given arguments. Passed as is to <code>vprintf</code>.
     * @param array  $arguments  array of arguments to pass to vsprinf
     * @param int    $debugLevel debug level at which to print this statement
     */
    public static function debug(string $format, array $arguments, int $debugLevel = self::DEBUG_LEVEL_LOW): bool
    {
        if (self::isDebug($debugLevel)) {
            vprintf($format."\n", $arguments);
        }

        return true;
    }

    /**
     * @return string[] array of hex representation of each byte of $string
     */
    public static function hexArray(string $string): array
    {
        return self::bytesArray($string);
    }

    /**
     * @return string hex-represented bytes of each byte of $string joined by $joiner
     */
    public static function hexString(string $string, string $joiner = ' '): string
    {
        return implode($joiner, self::hexArray($string));
    }

    /**
     * @return string[] array of each byte of $string formatted using $format
     */
    public static function bytesArray(string $string, string $format = 'x%02x'): array
    {
        $x = [];
        foreach (str_split($string) as $byte) {
            $x[] = sprintf($format, ord($byte));
        }

        return $x;
    }

    /**
     * @return string[] array of bytes of $string represented in decimal format ('%3d')
     */
    public static function decArray(string $string): array
    {
        return self::bytesArray($string, '%3d');
    }

    /**
     * @return string of bytes of $string represented in decimal format
     */
    public static function decString(string $string, string $joiner = ' '): string
    {
        return implode($joiner, self::decArray($string));
    }

    /**
     * @param string $format one of 'ctrl', 'hex', or 'dec' for control, hexadecimal, or decimal format for bytes.
     *                       - ctrl: ASCII control characters represented as text.
     *                       For example, the null byte is represented as 'NUL'. Visible ASCII characters represent
     *                       themselves, and others are represented as a decimal ('%03d')
     *                       - hex: bytes represented in hexadecimal ('%02X')
     *                       - dec: bytes represented in decimal ('%03d')
     *
     * @return string[] array of bytes represented in the given format
     */
    public static function asciiArray(string $string, string $format = 'ctrl'): array
    {
        if (!in_array($format, ['ctrl', 'hex', 'dec'])) {
            throw new Exception('Unrecognized format specifier');
        }
        $ctrlChars = [
            'NUL', 'SOH', 'STX', 'ETX', 'EOT', 'ENQ', 'ACK', 'BEL', 'BS', 'HT', 'LF', 'VT', 'FF', 'CR', 'SO', 'SI',
            'DLE', 'DC1', 'DC2', 'DC3', 'DC4', 'NAK', 'SYN', 'ETB', 'CAN', 'EM', 'SUB', 'ESC', 'FS', 'GS', 'RS', 'US',
        ];
        $matches = [];
        foreach (str_split($string) as $byte) {
            $decimalByte = ord($byte);
            if ($decimalByte < 32) {
                switch ($format) {
                    case 'ctrl':
                        $matches[] = str_pad($ctrlChars[$decimalByte], 3, ' ', STR_PAD_LEFT);
                        break;
                    case 'hex':
                        $matches[] = sprintf('x%02X', $decimalByte);
                        break;
                    case 'dec':
                        $matches[] = str_pad($decimalByte, 3, '0', STR_PAD_LEFT);
                        break;
                }
            } elseif ($decimalByte < 127) {
                $matches[] = sprintf('  %s', $byte);
            } elseif (127 === $decimalByte) {
                switch ($format) {
                    case 'ctrl':
                        $matches[] = 'DEL';
                        break;
                    case 'hex':
                        $matches[] = sprintf('x%02X', $decimalByte);
                        break;
                    case 'dec':
                        $matches[] = str_pad($decimalByte, 3, '0', STR_PAD_LEFT);
                        break;
                }
            } elseif ('hex' === $format) {
                $matches[] = sprintf('x%02X', $decimalByte);
            } else {
                $matches[] = str_pad($decimalByte, 3, '0', STR_PAD_LEFT);
            }
        }

        return $matches;
    }

    /**
     * @param string $format one of 'ctrl', 'hex', or 'dec'
     */
    public static function asciiString(string $string, string $format = 'ctrl', string $joiner = ' '): string
    {
        return implode($joiner, self::asciiArray($string, $format));
    }

    /**
     * Checks if the given $debugLevel is equivalent or more verbose than than the current debug level.
     */
    private static function isDebug(int $debugLevel = self::DEBUG_LEVEL_LOW): bool
    {
        return self::$debugLevel >= $debugLevel;
    }
}
