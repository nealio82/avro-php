<?php

namespace Avro\IO;

use Avro\Exception\IOException;

/**
 * IO wrapper for string access.
 */
class StringIO implements IO
{
    /**
     * @var string
     */
    private $string_buffer;
    /**
     * @var int current position in string
     */
    private $current_index;
    /**
     * @var bool whether or not the string is closed
     */
    private $is_closed;

    /**
     * @param string $str initial value of StringIO buffer. Regardless
     *                    of the initial value, the pointer is set to the
     *                    beginning of the buffer.
     *
     * @throws IOException if a non-string value is passed as $str
     */
    public function __construct($str = '')
    {
        $this->is_closed = false;
        $this->string_buffer = '';
        $this->current_index = 0;

        if (is_string($str)) {
            $this->string_buffer .= $str;
        } else {
            throw new IOException(
                sprintf('constructor argument must be a string: %s', gettype($str)));
        }
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return $this->string_buffer;
    }

    /**
     * Append bytes to this buffer.
     * (Nothing more is needed to support Avro.).
     *
     * @param string $arg bytes to write
     *
     * @throws IOException if $args is not a string value
     *
     * @return int count of bytes written
     */
    public function write($arg)
    {
        $this->check_closed();
        if (is_string($arg)) {
            return $this->append_str($arg);
        }
        throw new IOException(
            sprintf('write argument must be a string: (%s) %s',
                gettype($arg), var_export($arg, true)));
    }

    /**
     * @todo test for fencepost errors wrt updating current_index
     *
     * @param mixed $len
     *
     * @return string bytes read from buffer
     */
    public function read($len)
    {
        $this->check_closed();
        $read = '';

        for ($i = $this->current_index; $i < ($this->current_index + $len); ++$i) {
            $read .= $this->string_buffer[$i];
        }
        if (strlen($read) < $len) {
            $this->current_index = $this->length();
        } else {
            $this->current_index += $len;
        }

        return $read;
    }

    /**
     * @param mixed $offset
     * @param mixed $whence
     *
     * @throws IOException if the seek failed
     *
     * @return bool true if successful
     */
    public function seek($offset, $whence = self::SEEK_SET)
    {
        if (!is_int($offset)) {
            throw new IOException('Seek offset must be an integer.');
        }
        // Prevent seeking before BOF
        switch ($whence) {
            case self::SEEK_SET:
                if (0 > $offset) {
                    throw new IOException('Cannot seek before beginning of file.');
                }
                $this->current_index = $offset;
                break;
            case self::SEEK_CUR:
                if (0 > $this->current_index + $whence) {
                    throw new IOException('Cannot seek before beginning of file.');
                }
                $this->current_index += $offset;
                break;
            case self::SEEK_END:
                if (0 > $this->length() + $offset) {
                    throw new IOException('Cannot seek before beginning of file.');
                }
                $this->current_index = $this->length() + $offset;
                break;
            default:
                throw new IOException(sprintf('Invalid seek whence %d', $whence));
        }

        return true;
    }

    /**
     * @return int
     *
     * @see IO::tell()
     */
    public function tell()
    {
        return $this->current_index;
    }

    /**
     * @return bool
     *
     * @see IO::is_eof()
     */
    public function is_eof()
    {
        return $this->current_index >= $this->length();
    }

    /**
     * No-op provided for compatibility with IO interface.
     *
     * @return bool true
     */
    public function flush()
    {
        return true;
    }

    /**
     * Marks this buffer as closed.
     *
     * @return bool true
     */
    public function close()
    {
        $this->check_closed();
        $this->is_closed = true;

        return true;
    }

    /**
     * Truncates the truncate buffer to 0 bytes and returns the pointer
     * to the beginning of the buffer.
     *
     * @return bool true
     */
    public function truncate()
    {
        $this->check_closed();
        $this->string_buffer = '';
        $this->current_index = 0;

        return true;
    }

    /**
     * @return int count of bytes in the buffer
     *
     * @internal could probably memoize length for performance, but
     *           no need do this yet
     */
    public function length()
    {
        return strlen($this->string_buffer);
    }

    /**
     * @return string
     *
     * @uses \self::__toString()
     */
    public function string()
    {
        return $this->__toString();
    }

    /**
     * @return bool true if this buffer is closed and false
     *              otherwise
     */
    public function is_closed()
    {
        return $this->is_closed;
    }

    /**
     * @throws IOException if the buffer is closed
     */
    private function check_closed(): void
    {
        if ($this->is_closed()) {
            throw new IOException('Buffer is closed');
        }
    }

    /**
     * Appends bytes to this buffer.
     *
     * @param string $str
     *
     * @return int count of bytes written
     */
    private function append_str($str)
    {
        $this->check_closed();
        $isNew = false;
        if (empty($this->string_buffer)) {
            $isNew = true;
        }
        $this->string_buffer .= $str;
        $len = strlen($str);
        if (!$isNew) {
            $this->current_index += $len;
        }

        return $len;
    }
}
