<?php

namespace Avro\Datum;

use Avro\Avro;
use Avro\GMP\GMP;
use Avro\IO\IO;

/**
 * Decodes and reads Avro data from an IO object encoded using Avro binary encoding.
 */
class IOBinaryDecoder
{
    private $io;

    /**
     * @param IO $io object from which to read
     */
    public function __construct(IO $io)
    {
        Avro::checkPlatform();
        $this->io = $io;
    }

    /**
     * @internal Requires 64-bit platform
     *
     * @param int[] $bytes array of byte ascii values
     */
    public static function decodeLongFromArray(array $bytes): int
    {
        $byte = array_shift($bytes);
        $number = $byte & 0x7f;
        $shift = 7;
        while (0 !== ($byte & 0x80)) {
            $byte = array_shift($bytes);
            $number |= (($byte & 0x7f) << $shift);
            $shift += 7;
        }

        return ($number >> 1) ^ -($number & 1);
    }

    /**
     * Performs decoding of the binary string to a float value.
     *
     * This is <b>not</b> endian-aware! See comments in {@link IOBinaryEncoder::float_to_int_bits()} for details.
     */
    public static function intBitsToFloat(string $bits): float
    {
        $float = unpack('f', $bits);

        return (float) $float[1];
    }

    /**
     * Performs decoding of the binary string to a double value.
     *
     * This is <b>not</b> endian-aware! See comments in {@link IOBinaryEncoder::float_to_int_bits()} for details.
     */
    public static function longBitsToDouble(string $bits): float
    {
        $double = unpack('d', $bits);

        return (float) $double[1];
    }

    public function readNull()
    {
        return null;
    }

    public function readBoolean(): bool
    {
        return 1 === ord($this->nextByte());
    }

    public function readInt(): int
    {
        return $this->readLong();
    }

    public function readLong(): int
    {
        $byte = ord($this->nextByte());
        $bytes = [$byte];

        while (0 !== ($byte & 0x80)) {
            $byte = ord($this->nextByte());
            $bytes[] = $byte;
        }

        if (Avro::usesGmp()) {
            return GMP::decodeLongFromArray($bytes);
        }

        return self::decodeLongFromArray($bytes);
    }

    public function readFloat(): float
    {
        return self::intBitsToFloat($this->read(4));
    }

    public function readDouble(): float
    {
        return self::longBitsToDouble($this->read(8));
    }

    /**
     * A string is encoded as a long followed by that many bytes of UTF-8 encoded character data.
     */
    public function readString(): string
    {
        return $this->readBytes();
    }

    public function readBytes(): string
    {
        return $this->read($this->readLong());
    }

    public function read(int $length): string
    {
        return $this->io->read($length);
    }

    public function skipNull(): void
    {
    }

    public function skipBoolean(): void
    {
        $this->skip(1);
    }

    public function skipInt(): void
    {
        $this->skipLong();
    }

    public function skipLong(): void
    {
        $byte = ord($this->nextByte());
        while (0 !== ($byte & 0x80)) {
            $byte = $this->nextByte();
        }
    }

    public function skipFloat(): void
    {
        $this->skip(4);
    }

    public function skipDouble(): void
    {
        $this->skip(8);
    }

    public function skipBytes(): void
    {
        $this->skip($this->readLong());
    }

    public function skipString(): void
    {
        $this->skipBytes();
    }

    public function skip(int $length): void
    {
        $this->seek($length, IO::SEEK_CUR);
    }

    private function nextByte(): string
    {
        return $this->read(1);
    }

    private function tell(): int
    {
        return $this->io->tell();
    }

    private function seek(int $offset, int $whence): bool
    {
        return $this->io->seek($offset, $whence);
    }
}
