<?php

namespace Avro\IO;

use Avro\Exception\NotImplementedException;

/**
 * Barebones IO base class to provide common interface for file and string
 * access within the Avro classes.
 *
 * @package Avro
 */
class IO
{

    /**
     * @var string general read mode
     */
    const READ_MODE = 'r';
    /**
     * @var string general write mode.
     */
    const WRITE_MODE = 'w';

    /**
     * @var int set position equal to $offset bytes
     */
    const SEEK_CUR = SEEK_CUR;
    /**
     * @var int set position to current index + $offset bytes
     */
    const SEEK_SET = SEEK_SET;
    /**
     * @var int set position to end of file + $offset bytes
     */
    const SEEK_END = SEEK_END;

    /**
     * Read $len bytes from IO instance
     * @var int $len
     * @return string bytes read
     */
    public function read($len)
    {
        throw new NotImplementedException('Not implemented');
    }

    /**
     * Append bytes to this buffer. (Nothing more is needed to support Avro.)
     * @param str $arg bytes to write
     * @returns int count of bytes written.
     * @throws IOException if $args is not a string value.
     */
    public function write($arg)
    {
        throw new NotImplementedException('Not implemented');
    }

    /**
     * Return byte offset within IO instance
     * @return int
     */
    public function tell()
    {
        throw new NotImplementedException('Not implemented');
    }

    /**
     * Set the position indicator. The new position, measured in bytes
     * from the beginning of the file, is obtained by adding $offset to
     * the position specified by $whence.
     *
     * @param int $offset
     * @param int $whence one of AvroIO::SEEK_SET, AvroIO::SEEK_CUR,
     *                    or Avro::SEEK_END
     * @returns boolean true
     *
     * @throws IOException
     */
    public function seek($offset, $whence=self::SEEK_SET)
    {
        throw new NotImplementedException('Not implemented');
    }

    /**
     * Flushes any buffered data to the IO object.
     * @returns boolean true upon success.
     */
    public function flush()
    {
        throw new NotImplementedException('Not implemented');
    }

    /**
     * Returns whether or not the current position at the end of this IO
     * instance.
     *
     * Note is_eof() is <b>not</b> like eof in C or feof in PHP:
     * it returns TRUE if the *next* read would be end of file,
     * rather than if the *most recent* read read end of file.
     * @returns boolean true if at the end of file, and false otherwise
     */
    public function is_eof()
    {
        throw new NotImplementedException('Not implemented');
    }

    /**
     * Closes this IO instance.
     */
    public function close()
    {
        throw new NotImplementedException('Not implemented');
    }

}