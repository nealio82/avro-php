<?php

namespace Avro\IO;

use Avro\Exception\IOException;

/**
 * IO wrapper for PHP file access functions.
 */
class File implements IO
{
    private const FOPEN_READ_MODE = 'rb';
    private const FOPEN_WRITE_MODE = 'wb';

    private $fileHandle;

    public function __construct($filePath, $mode = self::READ_MODE)
    {
        switch ($mode) {
            case self::WRITE_MODE:
                $this->fileHandle = fopen($filePath, self::FOPEN_WRITE_MODE);
                if (false === $this->fileHandle) {
                    throw new IOException('Could not open file for writing');
                }
                break;
            case self::READ_MODE:
                // @todo: should we check for file existence (in case of reading) or anything else about the provided filePath argument?
                $this->fileHandle = fopen($filePath, self::FOPEN_READ_MODE);
                if (false === $this->fileHandle) {
                    throw new IOException('Could not open file for reading');
                }
                break;
            default:
                throw new IOException(
                    sprintf(
                        'Only modes "%s" and "%s" allowed. You provided "%s".',
                        self::READ_MODE,
                        self::WRITE_MODE,
                        $mode
                    )
                );
        }
    }

    public function read(int $length): string
    {
        if ($length < 0) {
            throw new IOException(sprintf('Invalid length value passed to read: %d', $length));
        }

        if (0 === $length) {
            return '';
        }

        $bytes = fread($this->fileHandle, $length);
        if (false === $bytes) {
            throw new IOException('Could not read from file');
        }

        return $bytes;
    }

    public function write(string $string): int
    {
        $length = fwrite($this->fileHandle, $string);
        if (false === $length) {
            throw new IOException(sprintf('Could not write to file'));
        }

        return $length;
    }

    public function tell(): int
    {
        $position = ftell($this->fileHandle);
        if (false === $position) {
            throw new IOException('Could not execute tell on reader');
        }

        return $position;
    }

    public function seek(int $offset, int $whence = self::SEEK_SET): bool
    {
        // Note: does not catch seeking beyond end of file
        if (fseek($this->fileHandle, $offset, $whence) === -1) {
            throw new IOException(sprintf('Could not execute seek (offset = %d, whence = %d)', $offset, $whence));
        }

        return true;
    }

    public function flush(): bool
    {
        if (!fflush($this->fileHandle)) {
            throw new IOException('Could not flush file.');
        }

        return true;
    }

    public function isEof(): bool
    {
        $this->read(1);
        if (feof($this->fileHandle)) {
            return true;
        }
        $this->seek(-1, self::SEEK_CUR);

        return false;
    }

    public function close(): bool
    {
        if (!fclose($this->fileHandle)) {
            throw new IOException('Error closing file.');
        }

        return true;
    }
}
