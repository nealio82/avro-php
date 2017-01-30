<?php

namespace Avro\DataIO;

use Avro\Datum\IOBinaryDecoder;
use Avro\Datum\IODatumReader;
use Avro\Exception\DataIoException;
use Avro\IO\IO;
use Avro\Schema\Schema;
use Avro\Util\Util;

/**
 *
 * Reads Avro data from an IO source using an AvroSchema.
 * @package Avro
 */
class DataIOReader
{
    /**
     * @var IO
     */
    private $io;

    /**
     * @var IOBinaryDecoder
     */
    private $decoder;

    /**
     * @var IODatumReader
     */
    private $datum_reader;

    /**
     * @var string
     */
    private $sync_marker;

    /**
     * @var array object container metadata
     */
    private $metadata;

    /**
     * @var int count of items in block
     */
    private $block_count;

    /**
     * @param IO $io source from which to read
     * @param IODatumReader $datum_reader reader that understands
     *                                        the data schema
     * @throws DataIoException if $io is not an instance of IO
     * @uses read_header()
     */
    public function __construct(IO $io, IODatumReader $datum_reader)
    {

        if (!($io instanceof IO)) {
            throw new DataIoException('io must be instance of IO');
        }

        $this->io = $io;
        $this->decoder = new IOBinaryDecoder($this->io);
        $this->datum_reader = $datum_reader;
        $this->read_header();

        $codec = Util::array_value($this->metadata,
            DataIO::METADATA_CODEC_ATTR);
        if ($codec && !DataIO::is_valid_codec($codec)) {
            throw new DataIoException(sprintf('Uknown codec: %s', $codec));
        }

        $this->block_count = 0;
        // FIXME: Seems unsanitary to set writers_schema here.
        // Can't constructor take it as an argument?
        $this->datum_reader->set_writers_schema(
            Schema::parse($this->metadata[DataIO::METADATA_SCHEMA_ATTR]));
    }

    /**
     * Reads header of object container
     * @throws DataIoException if the file is not an Avro data file.
     */
    private function read_header()
    {
        $this->seek(0, IO::SEEK_SET);

        $magic = $this->read(DataIO::magic_size());

        if (strlen($magic) < DataIO::magic_size())
            throw new DataIoException(
                'Not an Avro data file: shorter than the Avro magic block');

        if (DataIO::magic() != $magic) {

            throw new DataIoException(
                sprintf('Not an Avro data file: %s does not match %s',
                    $magic, DataIO::magic()));
        }

        $this->metadata = $this->datum_reader->read_data(DataIO::metadata_schema(),
            DataIO::metadata_schema(),
            $this->decoder);
        $this->sync_marker = $this->read(DataIO::SYNC_SIZE);
    }

    /**
     * @internal Would be nice to implement data() as an iterator, I think
     * @returns \Generator
     */
    public function data()
    {
        while (true) {
            if (0 == $this->block_count) {
                if ($this->is_eof()) {
                    break;
                }

                if ($this->skip_sync()) {
                    if ($this->is_eof()) {
                        break;
                    }
                }

                $this->read_block_header();
            }
            $data = $this->datum_reader->read($this->decoder);
            $this->block_count -= 1;
            yield $data;
        }
    }

    /**
     * Closes this writer (and its IO object.)
     * @uses IO::close()
     */
    public function close()
    {
        return $this->io->close();
    }

    /**
     * @uses IO::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    public function getSyncMarker()
    {
        return $this->sync_marker;
    }

    public function getMetaDataFor($key)
    {
        return $this->metadata[$key];
    }

    /**
     * @uses IO::read()
     */
    private function read($len)
    {
        return $this->io->read($len);
    }

    /**
     * @uses IO::is_eof()
     */
    private function is_eof()
    {
        return $this->io->is_eof();
    }

    private function skip_sync()
    {
        $proposed_sync_marker = $this->read(DataIO::SYNC_SIZE);
        if ($proposed_sync_marker != $this->sync_marker) {
            $this->seek(-DataIO::SYNC_SIZE, IO::SEEK_CUR);
            return false;
        }
        return true;
    }

    /**
     * Reads the block header (which includes the count of items in the block
     * and the length in bytes of the block)
     * @returns int length in bytes of the block.
     */
    private function read_block_header()
    {
        $this->block_count = $this->decoder->read_long();
        return $this->decoder->read_long();
    }

}