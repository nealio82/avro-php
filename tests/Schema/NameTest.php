<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use PHPUnit\Framework\TestCase;

class NameTest extends TestCase
{
    /**
     * @dataProvider nameProvider
     */
    public function testName(
        ?string $name,
        ?string $namespace,
        ?string $defaultNamespace,
        bool $isValid,
        ?string $expectedName,
        ?string $expectedNamespace,
        ?string $expectedFullname,
        ?string $expectedQualifiedName
    ): void {
        try {
            $nameClass = new Name($name, $namespace, $defaultNamespace);
            $this->assertTrue($isValid);
            $this->assertSame($expectedName, $nameClass->getName());
            $this->assertSame($expectedNamespace, $nameClass->getNamespace());
            $this->assertSame($expectedFullname, $nameClass->getFullname());
            $this->assertSame($expectedFullname, (string) $nameClass);
            $this->assertSame($expectedQualifiedName, $nameClass->getQualifiedName());
        } catch (SchemaParseException $e) {
            $this->assertFalse(
                $isValid,
                sprintf(
                    "%s:\n%s",
                    var_export([
                        'name' => $name,
                        'namespace' => $namespace,
                        'default_namespace' => $defaultNamespace,
                        'is_valid' => $isValid,
                        'expected_fullname' => $expectedFullname,
                    ], true),
                    $e->getMessage()
                )
            );
        }
    }

    public function nameProvider(): iterable
    {
        // name namespace defaultNamespace isValid expectedName expectedNamespace expectedFullname expectedQualifiedName
        yield ['foo', null, null, true, 'foo', null, 'foo', 'foo'];
        yield ['foo', 'bar', null, true, 'foo', 'bar', 'bar.foo', 'bar.foo'];
        yield ['foo', null, 'bar', true, 'foo', 'bar', 'bar.foo', 'foo'];
        yield ['foo', 'baz', 'bar', true, 'foo', 'baz', 'baz.foo', 'baz.foo'];
        yield ['foo', 'bar', 'bar', true, 'foo', 'bar', 'bar.foo', 'foo'];
        yield ['bar.foo', null, null, true, 'foo', 'bar', 'bar.foo', 'bar.foo'];
        yield ['bar.foo', 'baz', null, true, 'foo', 'bar', 'bar.foo', 'bar.foo'];
        yield ['bar.foo', null, 'baz', true, 'foo', 'bar', 'bar.foo', 'bar.foo'];
        yield ['bar.foo', 'baz', 'qux', true, 'foo', 'bar', 'bar.foo', 'bar.foo'];
        yield ['bar.foo', 'baz', 'baz', true, 'foo', 'bar', 'bar.foo', 'bar.foo'];
        yield ['bar.foo', null, 'bar', true, 'foo', 'bar', 'bar.foo', 'foo'];
        yield ['bar.foo', 'bar', null, true, 'foo', 'bar', 'bar.foo', 'bar.foo'];
        yield ['bar.foo', 'bar', 'bar', true, 'foo', 'bar', 'bar.foo', 'foo'];
        yield ['_bar.foo', 'baz', null, true, 'foo', '_bar', '_bar.foo', '_bar.foo'];
        yield ['bar._foo', 'baz', null, true, '_foo', 'bar', 'bar._foo', 'bar._foo'];
        yield ['b4r.foo', 'baz', null, true, 'foo', 'b4r', 'b4r.foo', 'b4r.foo'];
        yield ['bar.f0o', 'baz', null, true, 'f0o', 'bar', 'bar.f0o', 'bar.f0o'];
        yield ['', null, null, false, null, null, null, null];
        yield ['foo', '', null, false, null, null, null, null];
        yield ['3bar.foo', 'baz', null, false, null, null, null, null];
        yield ['bar.3foo', 'baz', null, false, null, null, null, null];
        yield [' .foo', 'baz', null, false, null, null, null, null];
        yield ['bar. foo', 'baz', null, false, null, null, null, null];
        yield ['bar. ', 'baz', null, false, null, null, null, null];
        yield ['3bar', 'baz', null, false, null, null, null, null];
    }

    /**
     * @dataProvider nameFormatProvider
     */
    public function testNameFormat(?string $name, bool $isWellFormed): void
    {
        $this->assertEquals($isWellFormed, Name::isWellFormedName($name));
    }

    public function nameFormatProvider(): iterable
    {
        // name isWellFormed
        yield ['a', true];
        yield ['_', true];
        yield ['1a', false];
        yield ['', false];
        yield [' ', false];
        yield ['Cons', true];
        yield ['-A', false];
        yield ['0A', false];
        yield [',A', false];
        yield ['A-', false];
        yield ['z,', false];
        yield ['zzzzz...', false];
    }
}
