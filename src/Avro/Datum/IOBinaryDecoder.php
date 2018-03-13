<?php

namespace Avro\Datum;

use Avro\Avro;
use Avro\GMP\GMP;
use Avro\IO\IO;

/**
 * Decodes and reads Avro data from an IO object encoded using
 * Avro binary encoding.
 */
class IOBinaryDecoder
{
    /**
     * @var IO
     */
    private $io;

    /**
     * @param IO $io object from which to read
     */
    public function __construct(IO $io)
    {
        Avro::check_platform();
        $this->io = $io;
    }

    /**
     * @param int[] array of byte ascii values
     * @param mixed $bytes
     *
     * @return long decoded value
     *
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

        return ($n >> 1) ^ -($n & 1);
    }

    /**
     * Performs decoding of the binary string to a float value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link IOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param string $bits
     *
     * @return float
     */
    public static function int_bits_to_float($bits)
    {
        $float = unpack('f', $bits);

        return (float) $float[1];
    }

    /**
     * Performs decoding of the binary string to a double value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link IOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param string $bits
     *
     * @return float
     */
    public static function long_bits_to_double($bits)
    {
        $double = unpack('d', $bits);

        return (float) $double[1];
    }

    public function read_null()
    {
        return null;
    }

    /**
     * @return bool
     */
    public function read_boolean()
    {
        return (bool) (1 == ord($this->next_byte()));
    }

    /**
     * @return int
     */
    public function read_int()
    {
        return (int) $this->read_long();
    }

    /**
     * @return long
     */
    public function read_long()
    {
        $byte = ord($this->next_byte());
        $bytes = [$byte];

        while (0 != ($byte & 0x80)) {
            $byte = ord($this->next_byte());
            $bytes[] = $byte;
        }

        if (Avro::uses_gmp()) {
            return GMP::decode_long_from_array($bytes);
        }

        return self::decode_long_from_array($bytes);
    }

    /**
     * @return float
     */
    public function read_float()
    {
        return self::int_bits_to_float($this->read(4));
    }

    /**
     * @return float
     */
    public function read_double()
    {
        return self::long_bits_to_double($this->read(8));
    }

    /**
     * A string is encoded as a long followed by that many bytes
     * of UTF-8 encoded character data.
     *
     * @return string
     */
    public function read_string()
    {
        return $this->read_bytes();
    }

    /**
     * @return string
     */
    public function read_bytes()
    {
        return $this->read($this->read_long());
    }

    /**
     * @param int $len count of bytes to read
     *
     * @return string
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

    public function skip_long(): void
    {
        $b = $this->next_byte();
        while (0 != ($b & 0x80)) {
            $b = $this->next_byte();
        }
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
     *
     * @uses \IO::seek()
     */
    public function skip($len): void
    {
        $this->seek($len, IO::SEEK_CUR);
    }

    /**
     * @throws Exception if the next byte cannot be read
     *
     * @return string the next byte from $this->io
     */
    private function next_byte()
    {
        return $this->read(1);
    }

    /**
     * @return int position of pointer in IO instance
     *
     * @uses \IO::tell()
     */
    private function tell()
    {
        return $this->io->tell();
    }

    /**
     * @param int $offset
     * @param int $whence
     *
     * @return bool true upon success
     *
     * @uses \IO::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }
}
