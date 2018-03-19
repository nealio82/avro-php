<?php

namespace Avro\DataIO;

use Avro\Datum\IOBinaryDecoder;
use Avro\Datum\IODatumReader;
use Avro\Exception\DataIoException;
use Avro\IO\IO;
use Avro\Schema\Schema;
use Avro\Util\Util;

/**
 * Reads Avro data from an IO source using an AvroSchema.
 */
class DataIOReader
{
    private $io;
    private $decoder;
    private $datumReader;
    private $blockCount;

    /**
     * @var array
     */
    private $metadata;

    /**
     * @var string
     */
    private $syncMarker;

    public function __construct(IO $io, IODatumReader $datumReader)
    {
        $this->io = $io;
        $this->decoder = new IOBinaryDecoder($this->io);
        $this->datumReader = $datumReader;
        $this->blockCount = 0;
        $this->readHeader();

        $codec = Util::arrayValue($this->metadata, DataIO::METADATA_CODEC_ATTR);
        if ($codec && !DataIO::isValidCodec($codec)) {
            throw new DataIoException(sprintf('Uknown codec: %s', $codec));
        }

        // @todo Seems unsanitary to set writers_schema here. Can't constructor take it as an argument?
        $this->datumReader->setWritersSchema(Schema::parse($this->metadata[DataIO::METADATA_SCHEMA_ATTR]));
    }

    public function data(): iterable
    {
        $data = [];
        while (true) {
            if (0 === $this->blockCount) {
                if ($this->isEof()) {
                    break;
                }

                if ($this->skipSync() && $this->isEof()) {
                    break;
                }

                $this->readBlockHeader();
            }
            $data[] = $this->datumReader->read($this->decoder);
            --$this->blockCount;
        }

        return $data;
    }

    public function close(): bool
    {
        return $this->io->close();
    }

    public function getSyncMarker(): string
    {
        return $this->syncMarker;
    }

    public function getMetaDataFor(string $key)
    {
        return $this->metadata[$key];
    }

    private function readHeader(): void
    {
        $this->seek(0, IO::SEEK_SET);

        $magic = $this->read(DataIO::magicSize());

        if (strlen($magic) < DataIO::magicSize()) {
            throw new DataIoException('Not an Avro data file: shorter than the Avro magic block');
        }

        if (DataIO::magic() !== $magic) {
            throw new DataIoException(sprintf('Not an Avro data file: %s does not match %s', $magic, DataIO::magic()));
        }

        $this->metadata = $this->datumReader->readData(
            DataIO::metadataSchema(),
            DataIO::metadataSchema(),
            $this->decoder
        );
        $this->syncMarker = $this->read(DataIO::SYNC_SIZE);
    }

    private function seek(int $offset, int $whence): bool
    {
        return $this->io->seek($offset, $whence);
    }

    private function read(int $length): string
    {
        return $this->io->read($length);
    }

    private function isEof(): bool
    {
        return $this->io->isEof();
    }

    private function skipSync(): bool
    {
        $proposed_sync_marker = $this->read(DataIO::SYNC_SIZE);
        if ($proposed_sync_marker !== $this->syncMarker) {
            $this->seek(-DataIO::SYNC_SIZE, IO::SEEK_CUR);

            return false;
        }

        return true;
    }

    /**
     * Reads the block header (which includes the count of items in the block and the length in bytes of the block).
     */
    private function readBlockHeader(): int
    {
        $this->blockCount = $this->decoder->readLong();

        return $this->decoder->readLong();
    }
}
