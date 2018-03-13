<?php

namespace Avro\Datum;

use Avro\Avro;
use Avro\GMP\GMP;
use Avro\IO\IO;

/**
 * Encodes and writes Avro data to an IO object using
 * Avro binary encoding.
 */
class IOBinaryEncoder
{
    /**
     * @var IO
     */
    private $io;

    /**
     * @param IO $io object to which data is to be written
     */
    public function __construct(IO $io)
    {
        Avro::check_platform();
        $this->io = $io;
    }

    /**
     * Performs encoding of the given float value to a binary string.
     *
     * XXX: This is <b>not</b> endian-aware! The {@link Avro::check_platform()}
     * called in {@link IOBinaryEncoder::__construct()} should ensure the
     * library is only used on little-endian platforms, which ensure the little-endian
     * encoding required by the Avro spec.
     *
     * @param float $float
     *
     * @return string bytes
     *
     * @see Avro::check_platform()
     */
    public static function float_to_int_bits($float)
    {
        return pack('f', (float) $float);
    }

    /**
     * Performs encoding of the given double value to a binary string.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link IOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param float $double
     *
     * @return string bytes
     */
    public static function double_to_long_bits($double)
    {
        return pack('d', (float) $double);
    }

    /**
     * @param int|string $n
     *
     * @return string long $n encoded as bytes
     *
     * @internal this relies on 64-bit PHP
     */
    public static function encode_long($n)
    {
        $n = (int) $n;
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
     * @param null $datum actual value is ignored
     */
    public function write_null($datum)
    {
        return null;
    }

    /**
     * @param bool $datum
     */
    public function write_boolean($datum): void
    {
        $byte = $datum ? chr(1) : chr(0);
        $this->write($byte);
    }

    /**
     * @param int $datum
     */
    public function write_int($datum): void
    {
        $this->write_long($datum);
    }

    /**
     * @param int $n
     */
    public function write_long($n): void
    {
        if (Avro::uses_gmp()) {
            $this->write(GMP::encode_long($n));
        } else {
            $this->write(self::encode_long($n));
        }
    }

    /**
     * @param float $datum
     *
     * @uses \self::float_to_int_bits()
     */
    public function write_float($datum): void
    {
        $this->write(self::float_to_int_bits($datum));
    }

    /**
     * @param float $datum
     *
     * @uses \self::double_to_long_bits()
     */
    public function write_double($datum): void
    {
        $this->write(self::double_to_long_bits($datum));
    }

    /**
     * @param string $str
     *
     * @uses \self::write_bytes()
     */
    public function write_string($str): void
    {
        $this->write_bytes($str);
    }

    /**
     * @param string $bytes
     */
    public function write_bytes($bytes): void
    {
        $this->write_long(strlen($bytes));
        $this->write($bytes);
    }

    /**
     * @param string $datum
     */
    public function write($datum): void
    {
        $this->io->write($datum);
    }
}
