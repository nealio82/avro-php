<?php

namespace Avro\IO;

use Avro\Exception\IOException;

/**
 * IO wrapper for string access.
 */
class StringIO implements IO
{
    private $stringBuffer = '';
    private $currentIndex = 0;
    private $isClosed = false;

    public function __construct(string $string = '')
    {
        if (!is_string($string)) {
            throw new IOException(sprintf('constructor argument must be a string: %s', gettype($string)));
        }

        $this->stringBuffer .= $string;
    }

    public function __toString()
    {
        return $this->stringBuffer;
    }

    /**
     * @todo test for fencepost errors write updating currentIndex
     */
    public function read(int $length): string
    {
        $this->checkClosed();

        $read = '';
        for ($i = $this->currentIndex; $i < ($this->currentIndex + $length); ++$i) {
            $read .= $this->stringBuffer[$i];
        }

        if (strlen($read) < $length) {
            $this->currentIndex = $this->length();
        } else {
            $this->currentIndex += $length;
        }

        return $read;
    }

    public function write(string $string): int
    {
        $this->checkClosed();

        if (!is_string($string)) {
            throw new IOException(
                sprintf('write argument must be a string: (%s) %s', gettype($string), var_export($string, true))
            );
        }

        return $this->appendString($string);
    }

    public function tell(): int
    {
        return $this->currentIndex;
    }

    public function seek(int $offset, int $whence = self::SEEK_SET): bool
    {
        // Prevent seeking before BOF
        switch ($whence) {
            case self::SEEK_SET:
                if (0 > $offset) {
                    throw new IOException('Cannot seek before beginning of file.');
                }
                $this->currentIndex = $offset;
                break;
            case self::SEEK_CUR:
                if (0 > $this->currentIndex + $whence) {
                    throw new IOException('Cannot seek before beginning of file.');
                }
                $this->currentIndex += $offset;
                break;
            case self::SEEK_END:
                if (0 > $this->length() + $offset) {
                    throw new IOException('Cannot seek before beginning of file.');
                }
                $this->currentIndex = $this->length() + $offset;
                break;
            default:
                throw new IOException(sprintf('Invalid seek whence %d', $whence));
        }

        return true;
    }

    public function flush(): bool
    {
        return true;
    }

    public function isEof(): bool
    {
        return $this->currentIndex >= $this->length();
    }

    public function close(): bool
    {
        $this->checkClosed();
        $this->isClosed = true;

        return true;
    }

    /**
     * Truncates the truncate buffer to 0 bytes and returns the pointer to the beginning of the buffer.
     */
    public function truncate(): bool
    {
        $this->checkClosed();
        $this->stringBuffer = '';
        $this->currentIndex = 0;

        return true;
    }

    /**
     * Returns count of bytes in the buffer.
     *
     * @todo could probably memorize length for performance, but no need do this yet
     */
    public function length(): int
    {
        return strlen($this->stringBuffer);
    }

    public function string(): string
    {
        return $this->__toString();
    }

    public function is_closed(): bool
    {
        return $this->isClosed;
    }

    private function checkClosed(): void
    {
        if ($this->is_closed()) {
            throw new IOException('Buffer is closed');
        }
    }

    private function appendString(string $string): int
    {
        $this->checkClosed();
        $this->stringBuffer .= $string;
        $length = strlen($string);
        $this->currentIndex += $length;

        return $length;
    }
}
