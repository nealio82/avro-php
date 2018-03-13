<?php

namespace Avro\IO;

use Avro\Exception\IOException;

/**
 * Barebones IO base class to provide common interface for file and string
 * access within the Avro classes.
 */
interface IO
{
    /**
     * @var string general read mode
     */
    const READ_MODE = 'r';
    /**
     * @var string general write mode
     */
    const WRITE_MODE = 'w';

    /**
     * @var int set position equal to offset bytes
     */
    const SEEK_CUR = SEEK_CUR;
    /**
     * @var int set position to current index + offset bytes
     */
    const SEEK_SET = SEEK_SET;
    /**
     * @var int set position to end of file + offset bytes
     */
    const SEEK_END = SEEK_END;

    /**
     * Read $len bytes from IO instance.
     *
     * @param int $len
     *
     * @return string bytes read
     */
    public function read($len);

    /**
     * Append bytes to this buffer.
     *
     * @param string $arg bytes to write
     *
     * @throws IOException if $args is not a string value
     *
     * @return int count of bytes written
     */
    public function write($arg);

    /**
     * Return byte offset within IO instance.
     *
     * @return int
     */
    public function tell();

    /**
     * Set the position indicator. The new position, measured in bytes
     * from the beginning of the file, is obtained by adding $offset to
     * the position specified by $whence.
     *
     * @param int $offset
     * @param int $whence one of AvroIO::SEEK_SET, AvroIO::SEEK_CUR,
     *                    or Avro::SEEK_END
     *
     * @return bool true
     */
    public function seek($offset, $whence = self::SEEK_SET);

    /**
     * Flushes any buffered data to the IO object.
     *
     * @return bool true upon success
     */
    public function flush();

    /**
     * Returns whether or not the current position at the end of this IO
     * instance.
     * Note is_eof() is <b>not</b> like eof in C or feof in PHP:
     * it returns TRUE if the *next* read would be end of file,
     * rather than if the *most recent* read read end of file.
     *
     * @return bool true if at the end of file, and false otherwise
     */
    public function is_eof();

    /**
     * Closes this IO instance.
     */
    public function close();
}
