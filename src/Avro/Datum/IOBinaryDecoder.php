<?php

namespace Avro\Datum;

use Avro\Avro;
use Avro\GMP\GMP;
use Avro\IO\IO;

/**
 * Decodes and reads Avro data from an IO object encoded using
 * Avro binary encoding.
 *
 * @package Avro
 */
class IOBinaryDecoder
{

    /**
     * @param int[] array of byte ascii values
     * @returns long decoded value
     * @internal Requires 64-bit platform
     */
    public static function decode_long_from_array($bytes)
    {
        $b = array_shift($bytes);
        $n = $b & 0x7f;
        $shift = 7;
        while (0 != ($b & 0x80)) {
            $b = array_shift($bytes);
            $n |= (($b & 0x7f) << $shift);
            $shift += 7;
        }
        return (($n >> 1) ^ -($n & 1));
    }

    /**
     * Performs decoding of the binary string to a float value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link IOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param string $bits
     * @returns float
     */
    static public function int_bits_to_float($bits)
    {
        $float = unpack('f', $bits);
        return (float)$float[1];
    }

    /**
     * Performs decoding of the binary string to a double value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link IOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param string $bits
     * @returns float
     */
    static public function long_bits_to_double($bits)
    {
        $double = unpack('d', $bits);
        return (double)$double[1];
    }

    /**
     * @var IO
     */
    private $io;

    /**
     * @param IO $io object from which to read.
     */
    public function __construct(IO $io)
    {
        Avro::check_platform();
        $this->io = $io;
    }

    /**
     * @returns string the next byte from $this->io.
     * @throws Exception if the next byte cannot be read.
     */
    private function next_byte()
    {
        return $this->read(1);
    }

    /**
     * @returns null
     */
    public function read_null()
    {
        return null;
    }

    /**
     * @returns boolean
     */
    public function read_boolean()
    {
        return (boolean)(1 == ord($this->next_byte()));
    }

    /**
     * @returns int
     */
    public function read_int()
    {
        return (int)$this->read_long();
    }

    /**
     * @returns long
     */
    public function read_long()
    {
        $byte = ord($this->next_byte());
        $bytes = array($byte);

        while (0 != ($byte & 0x80)) {
            $byte = ord($this->next_byte());
            $bytes [] = $byte;
        }

        if (Avro::uses_gmp()) {
            return GMP::decode_long_from_array($bytes);
        }

        return self::decode_long_from_array($bytes);
    }

    /**
     * @returns float
     */
    public function read_float()
    {
        return self::int_bits_to_float($this->read(4));
    }

    /**
     * @returns double
     */
    public function read_double()
    {
        return self::long_bits_to_double($this->read(8));
    }

    /**
     * A string is encoded as a long followed by that many bytes
     * of UTF-8 encoded character data.
     * @returns string
     */
    public function read_string()
    {
        return $this->read_bytes();
    }

    /**
     * @returns string
     */
    public function read_bytes()
    {
        return $this->read($this->read_long());
    }

    /**
     * @param int $len count of bytes to read
     * @returns string
     */
    public function read($len)
    {
        return $this->io->read($len);
    }

    public function skip_null()
    {
        return null;
    }

    public function skip_boolean()
    {
        return $this->skip(1);
    }

    public function skip_int()
    {
        return $this->skip_long();
    }

    public function skip_long()
    {
        $b = $this->next_byte();
        while (0 != ($b & 0x80))
            $b = $this->next_byte();
    }

    public function skip_float()
    {
        return $this->skip(4);
    }

    public function skip_double()
    {
        return $this->skip(8);
    }

    public function skip_bytes()
    {
        return $this->skip($this->read_long());
    }

    public function skip_string()
    {
        return $this->skip_bytes();
    }

    /**
     * @param int $len count of bytes to skip
     * @uses IO::seek()
     */
    public function skip($len)
    {
        $this->seek($len, IO::SEEK_CUR);
    }

    /**
     * @returns int position of pointer in IO instance
     * @uses IO::tell()
     */
    private function tell()
    {
        return $this->io->tell();
    }

    /**
     * @param int $offset
     * @param int $whence
     * @returns boolean true upon success
     * @uses IO::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }
}