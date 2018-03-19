<?php

namespace Avro\DataIO;

use Avro\Datum\IODatumReader;
use Avro\Datum\IODatumWriter;
use Avro\Exception\DataIoException;
use Avro\IO\File;
use Avro\IO\IO;
use Avro\Schema\Schema;

class DataIO
{
    /**
     * Count of bytes in synchronization marker.
     */
    public const SYNC_SIZE = 16;

    /**
     * Count of items per block, arbitrarily set to 4000 * SYNC_SIZE.
     *
     * @todo make this value configurable
     */
    public const SYNC_INTERVAL = 64000;

    /**
     * Map key for datafile metadata codec value.
     */
    public const METADATA_CODEC_ATTR = 'avro.codec';

    /**
     * Map key for datafile metadata schema value.
     */
    public const METADATA_SCHEMA_ATTR = 'avro.schema';

    public const NULL_CODEC = 'null';
    public const DEFLATE_CODEC = 'deflate';

    private const VERSION = 1;
    private const METADATA_SCHEMA_JSON = '{"type":"map","values":"bytes"}';

    /**
     * @todo Avro implementations are required to implement deflate codec as well, so implement it already!
     */
    private static $validCodecs = [self::NULL_CODEC];

    /**
     * @var Schema|null
     */
    private static $metadataSchema;

    /**
     * Returns the initial "magic" segment of an Avro container file header.
     */
    public static function magic(): string
    {
        return 'Obj'.pack('c', self::VERSION);
    }

    /**
     * Returns count of bytes in the initial "magic" segment of the Avro container file header.
     */
    public static function magicSize(): int
    {
        return strlen(self::magic());
    }

    /**
     * Returns Schema object of Avro container file metadata.
     */
    public static function metadataSchema(): Schema
    {
        if (null === self::$metadataSchema) {
            self::$metadataSchema = Schema::parse(self::METADATA_SCHEMA_JSON);
        }

        return self::$metadataSchema;
    }

    /**
     * @return DataIOReader|DataIOWriter
     */
    public static function openFile(string $filePath, string $mode = File::READ_MODE, ?string $schemaJson = null)
    {
        $schema = null !== $schemaJson ? Schema::parse($schemaJson) : null;

        if (File::WRITE_MODE === $mode) {
            if (null === $schema) {
                throw new DataIOException('Writing an Avro file requires a schema.');
            }

            return self::openWriter(new File($filePath, File::WRITE_MODE), $schema);
        }

        if (File::READ_MODE === $mode) {
            return self::openReader(new File($filePath, File::READ_MODE), $schema);
        }

        throw new DataIOException(
            sprintf('Only modes "%s" and "%s" allowed. You gave "%s".', File::READ_MODE, File::WRITE_MODE, $mode)
        );
    }

    public static function isValidCodec(string $codec): bool
    {
        return in_array($codec, self::$validCodecs, true);
    }

    private static function openWriter(IO $io, Schema $schema): DataIOWriter
    {
        return new DataIOWriter($io, new IODatumWriter($schema), $schema);
    }

    private static function openReader(IO $io, Schema $schema = null): DataIOReader
    {
        return new DataIOReader($io, new IODatumReader(null, $schema));
    }
}
