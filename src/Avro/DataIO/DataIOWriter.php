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
 * Writes Avro data to an AvroIO source using an AvroSchema.
 */
class DataIOWriter
{
    private $io;
    private $encoder;
    private $datumWriter;
    private $buffer;
    private $bufferEncoder;
    private $blockCount;
    private $metadata;
    private $syncMarker;

    public function __construct(IO $io, IODatumWriter $datumWriter, Schema $writersSchema = null)
    {
        $this->io = $io;
        $this->encoder = new IOBinaryEncoder($this->io);
        $this->datumWriter = $datumWriter;
        $this->buffer = new StringIO();
        $this->bufferEncoder = new IOBinaryEncoder($this->buffer);
        $this->blockCount = 0;
        $this->metadata = [];

        if ($writersSchema) {
            $this->syncMarker = self::generateSyncMarker();
            $this->metadata[DataIO::METADATA_CODEC_ATTR] = DataIO::NULL_CODEC;
            $this->metadata[DataIO::METADATA_SCHEMA_ATTR] = (string) $writersSchema;
            $this->writeHeader();
        } else {
            $dataIOReader = new DataIOReader($this->io, new IODatumReader());
            $this->syncMarker = $dataIOReader->getSyncMarker();
            $this->metadata[DataIO::METADATA_CODEC_ATTR] = $dataIOReader->getMetaDataFor(DataIO::METADATA_CODEC_ATTR);

            $schemaFromFile = $dataIOReader->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR);
            $this->metadata[DataIO::METADATA_SCHEMA_ATTR] = $schemaFromFile;
            $this->datumWriter = new IODatumWriter(Schema::parse($schemaFromFile));
            $this->seek(0, IO::SEEK_END);
        }
    }

    /**
     * @param mixed $datum
     */
    public function append($datum): void
    {
        $this->datumWriter->write($datum, $this->bufferEncoder);
        ++$this->blockCount;

        if ($this->buffer->length() >= DataIO::SYNC_INTERVAL) {
            $this->writeBlock();
        }
    }

    /**
     * Flushes buffer to IO object container and closes it.
     */
    public function close(): bool
    {
        $this->flush();

        return $this->io->close();
    }

    private static function generateSyncMarker(): string
    {
        // From http://php.net/manual/en/function.mt-rand.php comments
        return pack(
            'S8',
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff) | 0x4000,
            mt_rand(0, 0xffff) | 0x8000,
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff)
        );
    }

    private function flush(): bool
    {
        $this->writeBlock();

        return $this->io->flush();
    }

    /**
     * @todo: Should the codec check happen in the constructor? Why wait until we're writing data?
     */
    private function writeBlock(): void
    {
        if ($this->blockCount > 0) {
            $this->encoder->writeLong($this->blockCount);
            $toWrite = (string) $this->buffer;
            $this->encoder->writeLong(strlen($toWrite));

            if (!DataIO::isValidCodec($this->metadata[DataIO::METADATA_CODEC_ATTR])) {
                throw new DataIOException(
                    sprintf('Codec %s is not supported', $this->metadata[DataIO::METADATA_CODEC_ATTR])
                );
            }

            $this->write($toWrite);
            $this->write($this->syncMarker);
            $this->buffer->truncate();
            $this->blockCount = 0;
        }
    }

    private function writeHeader(): void
    {
        $this->write(DataIO::magic());
        $this->datumWriter->writeData(DataIO::metadataSchema(), $this->metadata, $this->encoder);
        $this->write($this->syncMarker);
    }

    private function write(string $bytes): int
    {
        return $this->io->write($bytes);
    }

    private function seek(int $offset, int $whence): bool
    {
        return $this->io->seek($offset, $whence);
    }
}
