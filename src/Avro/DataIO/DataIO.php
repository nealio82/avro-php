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
     * @var int used in file header
     */
    const VERSION = 1;

    /**
     * @var int count of bytes in synchronization marker
     */
    const SYNC_SIZE = 16;

    /**
     * @var int   count of items per block, arbitrarily set to 4000 * SYNC_SIZE
     * @todo make this value configurable
     */
    const SYNC_INTERVAL = 64000;

    /**
     * @var string map key for datafile metadata codec value
     */
    const METADATA_CODEC_ATTR = 'avro.codec';

    /**
     * @var string map key for datafile metadata schema value
     */
    const METADATA_SCHEMA_ATTR = 'avro.schema';
    /**
     * @var string JSON for datafile metadata schema
     */
    const METADATA_SCHEMA_JSON = '{"type":"map","values":"bytes"}';

    /**
     * @var string codec value for NULL codec
     */
    const NULL_CODEC = 'null';

    /**
     * @var string codec value for deflate codec
     */
    const DEFLATE_CODEC = 'deflate';

    /**
     * @var array array of valid codec names
     * @todo Avro implementations are required to implement deflate codec as well,
     *       so implement it already!
     */
    private static $valid_codecs = array(self::NULL_CODEC);

    /**
     * @var Schema cached version of metadata schema object
     */
    private static $metadata_schema;

    /**
     * @returns the initial "magic" segment of an Avro container file header.
     */
    public static function magic()
    {
        return ('Obj' . pack('c', self::VERSION));
    }

    /**
     * @returns int count of bytes in the initial "magic" segment of the
     *              Avro container file header
     */
    public static function magic_size()
    {
        return strlen(self::magic());
    }


    /**
     * @returns Schema object of Avro container file metadata.
     */
    public static function metadata_schema()
    {
        if (is_null(self::$metadata_schema)) {
            self::$metadata_schema = Schema::parse(self::METADATA_SCHEMA_JSON);
        }
        return self::$metadata_schema;
    }

    /**
     * @param string $file_path file_path of file to open
     * @param string $mode one of File::READ_MODE or File::WRITE_MODE
     * @param string $schema_json JSON of writer's schema
     * @returns DataIOWriter instance of DataIOWriter
     *
     * @throws DataIOException if $writers_schema is not provided
     *         or if an invalid $mode is given.
     */
    public static function open_file($file_path, $mode = File::READ_MODE,
                                     $schema_json = null)
    {
        $schema = !is_null($schema_json)
            ? Schema::parse($schema_json) : null;

        $io = false;
        switch ($mode) {
            case File::WRITE_MODE:
                if (is_null($schema)) {
                    throw new DataIOException('Writing an Avro file requires a schema.');
                }
                $file = new File($file_path, File::WRITE_MODE);
                $io = self::open_writer($file, $schema);
                break;
            case File::READ_MODE:
                $file = new File($file_path, File::READ_MODE);
                $io = self::open_reader($file, $schema);
                break;
            default:
                throw new DataIOException(
                    sprintf("Only modes '%s' and '%s' allowed. You gave '%s'.",
                        File::READ_MODE, File::WRITE_MODE, $mode));
        }
        return $io;
    }

    /**
     * @returns array array of valid codecs
     */
    private static function valid_codecs()
    {
        return self::$valid_codecs;
    }

    /**
     * @param string $codec
     * @returns boolean true if $codec is a valid codec value and false otherwise
     */
    public static function is_valid_codec($codec)
    {
        return in_array($codec, self::valid_codecs());
    }

    /**
     * @param IO $io
     * @param Schema $schema
     * @returns DataIOWriter
     */
    protected static function open_writer(IO $io, Schema $schema)
    {
        $writer = new IODatumWriter($schema);
        return new DataIOWriter($io, $writer, $schema);
    }

    /**
     * @param IO $io
     * @param Schema $schema
     * @returns DataIOReader
     */
    protected static function open_reader(IO $io, Schema $schema = null)
    {
        $reader = new IODatumReader(null, $schema);
        return new DataIOReader($io, $reader);
    }

}