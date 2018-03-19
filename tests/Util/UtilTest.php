<?php

namespace Avro\Util;

use PHPUnit\Framework\TestCase;

class UtilTest extends TestCase
{
    /**
     * @dataProvider getLists
     *
     * @param mixed $actual
     */
    public function testIsList($actual, bool $expected): void
    {
        $this->assertSame($expected, Util::isList($actual));
    }

    public function getLists(): iterable
    {
        yield [[1, 2, 'foo'], true];
        yield [['a' => 1, 'b' => 2, 'c' => 'foo'], false];
        yield ['', false];
        yield [123, false];
        yield [true, false];
        yield [null, false];
    }

    /**
     * @dataProvider getArrayValues
     *
     * @param mixed $array
     * @param mixed $key
     * @param mixed $expected
     */
    public function testArrayValue($array, $key, $expected): void
    {
        $this->assertSame($expected, Util::arrayValue($array, $key));
    }

    public function getArrayValues(): iterable
    {
        yield [['foo' => 'bar', 'baz' => 'qux'], 'foo', 'bar'];
        yield [['foo', 'bar'], 1, 'bar'];
        yield [['foo' => 'bar'], 'baz', null];
        yield [[], 0, null];
    }
}
