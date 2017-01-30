<?php

namespace Avro\Datum;

use Avro\Avro;
use Avro\GMP\GMP;
use Avro\IO\IO;

/**
 * Encodes and writes Avro data to an IO object using
 * Avro binary encoding.
 *
 * @package Avro
 */
class IOBinaryEncoder
{
    /**
     * Performs encoding of the given float value to a binary string
     *
     * XXX: This is <b>not</b> endian-aware! The {@link Avro::check_platform()}
     * called in {@link IOBinaryEncoder::__construct()} should ensure the
     * library is only used on little-endian platforms, which ensure the little-endian
     * encoding required by the Avro spec.
     *
     * @param float $float
     * @returns string bytes
     * @see Avro::check_platform()
     */
    static function float_to_int_bits($float)
    {
        return pack('f', (float)$float);
    }

    /**
     * Performs encoding of the given double value to a binary string
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link IOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param double $double
     * @returns string bytes
     */
    static function double_to_long_bits($double)
    {
        return pack('d', (double)$double);
    }

    /**
     * @param int|string $n
     * @returns string long $n encoded as bytes
     * @internal This relies on 64-bit PHP.
     */
    static public function encode_long($n)
    {
        $n = (int)$n;
        $n = ($n << 1) ^ ($n >> 63);
        $str = '';
        while (0 != ($n & ~0x7F)) {
            $str .= chr(($n & 0x7F) | 0x80);
            $n >>= 7;
        }
        $str .= chr($n);
        return $str;
    }

    /**
     * @var IO
     */
    private $io;

    /**
     * @param IO $io object to which data is to be written.
     *
     */
    function __construct(IO $io)
    {
        Avro::check_platform();
        $this->io = $io;
    }

    /**
     * @param null $datum actual value is ignored
     */
    function write_null($datum)
    {
        return null;
    }

    /**
     * @param boolean $datum
     */
    function write_boolean($datum)
    {
        $byte = $datum ? chr(1) : chr(0);
        $this->write($byte);
    }

    /**
     * @param int $datum
     */
    function write_int($datum)
    {
        $this->write_long($datum);
    }

    /**
     * @param int $n
     */
    function write_long($n)
    {
        if (Avro::uses_gmp())
            $this->write(GMP::encode_long($n));
        else
            $this->write(self::encode_long($n));
    }

    /**
     * @param float $datum
     * @uses self::float_to_int_bits()
     */
    public function write_float($datum)
    {
        $this->write(self::float_to_int_bits($datum));
    }

    /**
     * @param float $datum
     * @uses self::double_to_long_bits()
     */
    public function write_double($datum)
    {
        $this->write(self::double_to_long_bits($datum));
    }

    /**
     * @param string $str
     * @uses self::write_bytes()
     */
    function write_string($str)
    {
        $this->write_bytes($str);
    }

    /**
     * @param string $bytes
     */
    function write_bytes($bytes)
    {
        $this->write_long(strlen($bytes));
        $this->write($bytes);
    }

    /**
     * @param string $datum
     */
    function write($datum)
    {
        $this->io->write($datum);
    }
}