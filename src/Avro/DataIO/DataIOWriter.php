<?php

namespace Avro\DataIO;

use Avro\Datum\IOBinaryEncoder;
use Avro\Datum\IODatumReader;
use Avro\Datum\IODatumWriter;
use Avro\Exception\DataIoException;
use Avro\IO\IO;
use Avro\IO\StringIO;
use Avro\Schema\Schema;

/**
 * Writes Avro data to an AvroIO source using an AvroSchema
 * @package Avro
 */
class DataIOWriter
{
    /**
     * @returns string a new, unique sync marker.
     */
    private static function generate_sync_marker()
    {
        // From http://php.net/manual/en/function.mt-rand.php comments
        return pack('S8',
            mt_rand(0, 0xffff), mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff) | 0x4000,
            mt_rand(0, 0xffff) | 0x8000,
            mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff));
    }

    /**
     * @var IO object container where data is written
     */
    private $io;

    /**
     * @var IOBinaryEncoder encoder for object container
     */
    private $encoder;

    /**
     * @var AvroDatumWriter
     */
    private $datum_writer;

    /**
     * @var StringIO buffer for writing
     */
    private $buffer;

    /**
     * @var IOBinaryEncoder encoder for buffer
     */
    private $buffer_encoder; // IOBinaryEncoder

    /**
     * @var int count of items written to block
     */
    private $block_count;

    /**
     * @var array map of object container metadata
     */
    private $metadata;

    /**
     * @param IO $io
     * @param IODatumWriter $datum_writer
     * @param Schema $writers_schema
     */
    public function __construct(IO $io, IODatumWriter $datum_writer, Schema $writers_schema = null)
    {
        if (!($io instanceof IO)) {
            throw new DataIOException('io must be instance of IO');
        }

        $this->io = $io;
        $this->encoder = new IOBinaryEncoder($this->io);
        $this->datum_writer = $datum_writer;
        $this->buffer = new StringIO();
        $this->buffer_encoder = new IOBinaryEncoder($this->buffer);
        $this->block_count = 0;
        $this->metadata = array();

        if ($writers_schema) {
            $this->sync_marker = self::generate_sync_marker();
            $this->metadata[DataIO::METADATA_CODEC_ATTR] = DataIO::NULL_CODEC;
            $this->metadata[DataIO::METADATA_SCHEMA_ATTR] = strval($writers_schema);
            $this->write_header();
        } else {
            $dataIOReader = new DataIOReader($this->io, new IODatumReader());
            $this->sync_marker = $dataIOReader->getSyncMarker();
            $this->metadata[DataIO::METADATA_CODEC_ATTR] = $dataIOReader->getMetaDataFor(DataIO::METADATA_CODEC_ATTR);

            $schema_from_file = $dataIOReader->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR);
            $this->metadata[DataIO::METADATA_SCHEMA_ATTR] = $schema_from_file;
            $this->datum_writer = new IODatumWriter(Schema::parse($schema_from_file));
            $this->seek(0, SEEK_END);
        }
    }

    /**
     * @param mixed $datum
     */
    public function append($datum)
    {
        $this->datum_writer->write($datum, $this->buffer_encoder);
        $this->block_count++;

        if ($this->buffer->length() >= DataIO::SYNC_INTERVAL)
            $this->write_block();
    }

    /**
     * Flushes buffer to IO object container and closes it.
     * @return mixed value of $io->close()
     * @see IO::close()
     */
    public function close()
    {
        $this->flush();
        return $this->io->close();
    }

    /**
     * Flushes buffer to IO object container.
     * @returns mixed value of $io->flush()
     * @see IO::flush()
     */
    private function flush()
    {
        $this->write_block();
        return $this->io->flush();
    }

    /**
     * Writes a block of data to the IO object container.
     * @throws DataIOException if the codec provided by the encoder
     *         is not supported
     * @internal Should the codec check happen in the constructor?
     *           Why wait until we're writing data?
     */
    private function write_block()
    {
        if ($this->block_count > 0) {
            $this->encoder->write_long($this->block_count);
            $to_write = strval($this->buffer);
            $this->encoder->write_long(strlen($to_write));

            if (DataIO::is_valid_codec(
                $this->metadata[DataIO::METADATA_CODEC_ATTR])
            )
                $this->write($to_write);
            else
                throw new DataIOException(
                    sprintf('codec %s is not supported',
                        $this->metadata[DataIO::METADATA_CODEC_ATTR]));

            $this->write($this->sync_marker);
            $this->buffer->truncate();
            $this->block_count = 0;
        }
    }

    /**
     * Writes the header of the IO object container
     */
    private function write_header()
    {
        $this->write(DataIO::magic());
        $this->datum_writer->write_data(DataIO::metadata_schema(),
            $this->metadata, $this->encoder);
        $this->write($this->sync_marker);
    }

    /**
     * @param string $bytes
     * @uses IO::write()
     */
    private function write($bytes)
    {
        return $this->io->write($bytes);
    }

    /**
     * @param int $offset
     * @param int $whence
     * @uses IO::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }
}