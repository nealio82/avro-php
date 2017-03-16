# nealio82/avro-php

This is a package based on the [Apache AVRO PHP library](http://apache.mirror.anlx.net/avro/) (v 1.8.1).

The core logic remains the same but I've added Composer support, PSR-4 autoloading, method type-hinting, fixed property accessors with encapsulation where I've found errors (private/public member access), and some other tidy-ups.

## Usage

### Installation

```bash
$ composer require nealio82/avro-php
```

### Usage

Based on the official library's example:

```php
<?php

use Avro\DataIO\DataIO;
use Avro\DataIO\DataIOReader;
use Avro\DataIO\DataIOWriter;
use Avro\Datum\IODatumReader;
use Avro\Datum\IODatumWriter;
use Avro\IO\StringIO;
use Avro\Schema\Schema;

require_once('vendor/autoload.php');

$writers_schema_json = <<<_JSON
{
 "name":"member",
 "type":"record",
 "fields":
    [
        {"name":"member_id", "type":"int"},
        {"name":"member_name", "type":"string"}
    ]
}
_JSON;

$jose = array('member_id' => 1392, 'member_name' => 'Jose');
$maria = array('member_id' => 1642, 'member_name' => 'Maria');
$data = array($jose, $maria);


$file_name = 'data.avr';

// Open $file_name for writing, using the given writer's schema
$data_writer = DataIO::open_file($file_name, 'w', $writers_schema_json);
// Write each datum to the file
foreach ($data as $datum) {
    $data_writer->append($datum);
}
$data_writer->close();


// Open $file_name (by default for reading) using the writer's schema
// included in the file
$data_reader = DataIO::open_file($file_name);
echo "from file:\n";
// Read each datum
foreach ($data_reader->data() as $datum) {
    echo var_export($datum, true) . "\n";
}
$data_reader->close();


$io = new StringIO();

$writers_schema = Schema::parse($writers_schema_json);
$data_writer = new DataIOWriter($io, new IODatumWriter($writers_schema), $writers_schema);

foreach ($data as $datum) {
    $data_writer->append($datum);
}
$data_writer->close();


$binary_string = $io->string();

// Load the string data string
$read_io = new StringIO($binary_string);
$data_reader = new DataIOReader($read_io, new IODatumReader());
echo "from binary string:\n";
foreach ($data_reader->data() as $datum) {
    echo var_export($datum, true) . "\n";
}
```
