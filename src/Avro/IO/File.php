<?php

namespace Avro\IO;

use Avro\Exception\IOException;

/**
 * IO wrapper for PHP file access functions.
 */
class File implements IO
{
    /**
     * @var string fopen read mode value. Used internally.
     */
    const FOPEN_READ_MODE = 'rb';

    /**
     * @var string fopen write mode value. Used internally.
     */
    const FOPEN_WRITE_MODE = 'wb';

    /**
     * @var string
     */
    private $file_path;

    /**
     * @var resource file handle for File instance
     */
    private $file_handle;

    public function __construct($file_path, $mode = self::READ_MODE)
    {
        /*
         * XXX: should we check for file existence (in case of reading)
         * or anything else about the provided file_path argument?
         */
        $this->file_path = $file_path;
        switch ($mode) {
            case self::WRITE_MODE:
                $this->file_handle = fopen($this->file_path, self::FOPEN_WRITE_MODE);
                if (false == $this->file_handle) {
                    throw new IOException('Could not open file for writing');
                }
                break;
            case self::READ_MODE:
                $this->file_handle = fopen($this->file_path, self::FOPEN_READ_MODE);
                if (false == $this->file_handle) {
                    throw new IOException('Could not open file for reading');
                }
                break;
            default:
                throw new IOException(
                    sprintf("Only modes '%s' and '%s' allowed. You provided '%s'.",
                        self::READ_MODE, self::WRITE_MODE, $mode));
        }
    }

    /**
     * @param mixed $str
     *
     * @throws IOException if write failed
     *
     * @return int count of bytes written
     */
    public function write($str)
    {
        $len = fwrite($this->file_handle, $str);
        if (false === $len) {
            throw new IOException(sprintf('Could not write to file'));
        }

        return $len;
    }

    /**
     * @param int $len count of bytes to read
     *
     * @throws IOException if length value is negative or if the read failed
     *
     * @return string bytes read
     */
    public function read($len)
    {
        if (0 > $len) {
            throw new IOException(
                sprintf('Invalid length value passed to read: %d', $len));
        }

        if (0 == $len) {
            return '';
        }

        $bytes = fread($this->file_handle, $len);
        if (false === $bytes) {
            throw new IOException('Could not read from file');
        }

        return $bytes;
    }

    /**
     * @throws FileExcpetion if tell failed
     *
     * @return int current position within the file
     */
    public function tell()
    {
        $position = ftell($this->file_handle);
        if (false === $position) {
            throw new IOException('Could not execute tell on reader');
        }

        return $position;
    }

    /**
     * @param int $offset
     * @param int $whence
     *
     * @throws IOException if seek failed
     *
     * @return bool true upon success
     *
     * @see IO::seek()
     */
    public function seek($offset, $whence = SEEK_SET)
    {
        $res = fseek($this->file_handle, $offset, $whence);
        // Note: does not catch seeking beyond end of file
        if (-1 === $res) {
            throw new IOException(
                sprintf('Could not execute seek (offset = %d, whence = %d)',
                    $offset, $whence));
        }

        return true;
    }

    /**
     * Closes the file.
     *
     * @throws IOException if there was an error closing the file
     *
     * @return bool true if successful
     */
    public function close()
    {
        $res = fclose($this->file_handle);
        if (false === $res) {
            throw new IOException('Error closing file.');
        }

        return $res;
    }

    /**
     * @return bool true if the pointer is at the end of the file,
     *              and false otherwise
     *
     * @see IO::is_eof() as behavior differs from feof()
     */
    public function is_eof()
    {
        $this->read(1);
        if (feof($this->file_handle)) {
            return true;
        }
        $this->seek(-1, self::SEEK_CUR);

        return false;
    }

    /**
     * @throws IOException if there was an error flushing the file
     *
     * @return bool true if the flush was successful
     */
    public function flush()
    {
        $res = fflush($this->file_handle);
        if (false === $res) {
            throw new IOException('Could not flush file.');
        }

        return true;
    }
}
