<?php

namespace Avro\IO;

/**
 * Barebones IO base class to provide common interface for file and string access within the Avro classes.
 */
interface IO
{
    public const READ_MODE = 'r';
    public const WRITE_MODE = 'w';

    // Set position equal to offset bytes
    public const SEEK_CUR = SEEK_CUR;
    // Set position to current index + offset bytes
    public const SEEK_SET = SEEK_SET;
    // Set position to end of file + offset bytes
    public const SEEK_END = SEEK_END;

    /**
     * Read $length bytes from IO instance.
     */
    public function read(int $length): string;

    /**
     * Append $argument bytes to this buffer and returns the count of bytes written.
     */
    public function write(string $string): int;

    /**
     * Return byte offset within IO instance.
     */
    public function tell(): int;

    /**
     * Set the position indicator. The new position, measured in bytes from the beginning of the file,
     * is obtained by adding $offset to the position specified by $whence.
     */
    public function seek(int $offset, int $whence = self::SEEK_SET): bool;

    /**
     * Flushes any buffered data to the IO object.
     */
    public function flush(): bool;

    /**
     * Returns whether or not the current position at the end of this IO instance.
     */
    public function isEof(): bool;

    /**
     * Closes this IO instance.
     */
    public function close(): bool;
}
