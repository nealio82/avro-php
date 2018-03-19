<?php

namespace Avro\Datum;

use Avro\Avro;
use Avro\GMP\GMP;
use Avro\IO\IO;

/**
 * Encodes and writes Avro data to an IO object using Avro binary encoding.
 */
class IOBinaryEncoder
{
    private $io;

    /**
     * @param IO $io object to which data is to be written
     */
    public function __construct(IO $io)
    {
        Avro::checkPlatform();
        $this->io = $io;
    }

    /**
     * Performs encoding of the given float value to a binary string.
     *
     * This is <b>not</b> endian-aware! The {@link Avro::checkPlatform()} called in
     * {@link IOBinaryEncoder::__construct()} should ensure the library is only used on little-endian platforms,
     * which ensure the little-endian encoding required by the Avro spec.
     */
    public static function floatToIntBits(float $float): string
    {
        return pack('f', $float);
    }

    /**
     * Performs encoding of the given double value to a binary string.
     *
     * This is <b>not</b> endian-aware! See comments in {@link IOBinaryEncoder::floatToIntBits()} for details.
     */
    public static function doubleToLongBits(float $double): string
    {
        return pack('d', $double);
    }

    /**
     * @internal this relies on 64-bit PHP
     *
     * @param int|string $number
     */
    public static function encodeLong($number): string
    {
        $number = (int) $number;
        $number = ($number << 1) ^ ($number >> 63);
        $string = '';
        while (0 !== ($number & ~0x7F)) {
            $string .= chr(($number & 0x7F) | 0x80);
            $number >>= 7;
        }
        $string .= chr($number);

        return $string;
    }

    /**
     * @param null $datum actual value is ignored
     */
    public function writeNull($datum): void
    {
    }

    /**
     * @param bool $datum
     */
    public function writeBoolean($datum): void
    {
        $byte = $datum ? chr(1) : chr(0);
        $this->write($byte);
    }

    /**
     * @param int $datum
     */
    public function writeInt($datum): void
    {
        $this->writeLong($datum);
    }

    /**
     * @param int $number
     */
    public function writeLong($number): void
    {
        if (Avro::usesGmp()) {
            $this->write(GMP::encodeLong($number));
        } else {
            $this->write(self::encodeLong($number));
        }
    }

    /**
     * @param float $datum
     */
    public function writeFloat($datum): void
    {
        $this->write(self::floatToIntBits($datum));
    }

    /**
     * @param float $datum
     */
    public function writeDouble($datum): void
    {
        $this->write(self::doubleToLongBits($datum));
    }

    /**
     * @param string $string
     */
    public function writeString($string): void
    {
        $this->writeBytes($string);
    }

    /**
     * @param string $bytes
     */
    public function writeBytes($bytes): void
    {
        $this->writeLong(strlen($bytes));
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
