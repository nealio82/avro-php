<?php

namespace Avro\Datum;

use Avro\Debug\Debug;
use PHPUnit\Framework\TestCase;

class FloatIntEncodingTest extends TestCase
{
    public const FLOAT_TYPE = 'float';
    public const DOUBLE_TYPE = 'double';

    public static $floatNan;
    public static $floatPosInf;
    public static $floatNegInf;
    public static $doubleNan;
    public static $doublePosInf;
    public static $doubleNegInf;

    public static $longBitsNan;
    public static $longBitsPosInf;
    public static $longBitsNegInf;
    public static $intBitsNan;
    public static $intBitsPosInf;
    public static $intBitsNegInf;

    protected function setUp(): void
    {
        self::make_special_vals();
    }

    public static function make_special_vals(): void
    {
        self::$doubleNan = (float) NAN;
        self::$doublePosInf = (float) INF;
        self::$doubleNegInf = (float) -INF;
        self::$floatNan = (float) NAN;
        self::$floatPosInf = (float) INF;
        self::$floatNegInf = (float) -INF;

        self::$longBitsNan = strrev(pack('H*', '7ff8000000000000'));
        self::$longBitsPosInf = strrev(pack('H*', '7ff0000000000000'));
        self::$longBitsNegInf = strrev(pack('H*', 'fff0000000000000'));
        self::$intBitsNan = strrev(pack('H*', '7fc00000'));
        self::$intBitsPosInf = strrev(pack('H*', '7f800000'));
        self::$intBitsNegInf = strrev(pack('H*', 'ff800000'));
    }

    public function testSpecialValues(): void
    {
        $this->assertInternalType('float', self::$floatNan, 'float NaN is a float');
        $this->assertTrue(is_nan(self::$floatNan), 'float NaN is NaN');
        $this->assertFalse(is_infinite(self::$floatNan), 'float NaN is not infinite');

        $this->assertInternalType('float', self::$floatPosInf, 'float pos infinity is a float');
        $this->assertTrue(is_infinite(self::$floatPosInf), 'float pos infinity is infinite');
        $this->assertTrue(0 < self::$floatPosInf, 'float pos infinity is greater than 0');
        $this->assertFalse(is_nan(self::$floatPosInf), 'float pos infinity is not NaN');

        $this->assertInternalType('float', self::$floatNegInf, 'float neg infinity is a float');
        $this->assertTrue(is_infinite(self::$floatNegInf), 'float neg infinity is infinite');
        $this->assertTrue(0 > self::$floatNegInf, 'float neg infinity is less than 0');
        $this->assertFalse(is_nan(self::$floatNegInf), 'float neg infinity is not NaN');

        $this->assertInternalType('double', self::$doubleNan, 'double NaN is a double');
        $this->assertTrue(is_nan(self::$doubleNan), 'double NaN is NaN');
        $this->assertFalse(is_infinite(self::$doubleNan), 'double NaN is not infinite');

        $this->assertInternalType('double', self::$doublePosInf, 'double pos infinity is a double');
        $this->assertTrue(is_infinite(self::$doublePosInf), 'double pos infinity is infinite');
        $this->assertTrue(0 < self::$doublePosInf, 'double pos infinity is greater than 0');
        $this->assertFalse(is_nan(self::$doublePosInf), 'double pos infinity is not NaN');

        $this->assertInternalType('double', self::$doubleNegInf, 'double neg infinity is a double');
        $this->assertTrue(is_infinite(self::$doubleNegInf), 'double neg infinity is infinite');
        $this->assertTrue(0 > self::$doubleNegInf, 'double neg infinity is less than 0');
        $this->assertFalse(is_nan(self::$doubleNegInf), 'double neg infinity is not NaN');
    }

    /**
     * @dataProvider specialValuesProvider
     *
     * @param mixed $type
     * @param mixed $value
     * @param mixed $bits
     */
    public function testEncodingSpecialValues($type, $value, $bits): void
    {
        $this->assertEncodeValues($type, $value, $bits);
    }

    public function specialValuesProvider(): iterable
    {
        self::make_special_vals();

        yield [self::DOUBLE_TYPE, self::$doublePosInf, self::$longBitsPosInf];
        yield [self::DOUBLE_TYPE, self::$doubleNegInf, self::$longBitsNegInf];
        yield [self::FLOAT_TYPE, self::$floatPosInf, self::$intBitsPosInf];
        yield [self::FLOAT_TYPE, self::$floatNegInf, self::$intBitsNegInf];
    }

    /**
     * @dataProvider nanValuesProvider
     *
     * @param mixed $type
     * @param mixed $value
     * @param mixed $bits
     */
    public function testEncodingNanValues($type, $value, $bits): void
    {
        $this->assertEncodeNanValues($type, $value, $bits);
    }

    public function nanValuesProvider(): iterable
    {
        self::make_special_vals();

        yield [self::DOUBLE_TYPE, self::$doubleNan, self::$longBitsNan];
        yield [self::FLOAT_TYPE, self::$floatNan, self::$intBitsNan];
    }

    /**
     * @dataProvider floatValuesProvider
     *
     * @param mixed $type
     * @param mixed $value
     * @param mixed $bits
     */
    public function testEncodingFloatValues($type, $value, $bits): void
    {
        $this->assertEncodeValues($type, $value, $bits);
    }

    public function floatValuesProvider(): iterable
    {
        $ary = [];

        foreach ($this->normalValuesProvider() as $values) {
            if (self::FLOAT_TYPE === $values[0]) {
                $ary[] = [$values[0], $values[1], $values[2]];
            }
        }

        return $ary;
    }

    /**
     * @dataProvider doubleValuesProvider
     *
     * @param mixed $type
     * @param mixed $value
     * @param mixed $bits
     */
    public function testEncodingDoubleValues($type, $value, $bits): void
    {
        $this->assertEncodeValues($type, $value, $bits);
    }

    public function doubleValuesProvider(): iterable
    {
        $ary = [];

        foreach ($this->normalValuesProvider() as $values) {
            if (self::DOUBLE_TYPE === $values[0]) {
                $ary[] = [$values[0], $values[1], $values[2]];
            }
        }

        return $ary;
    }

    private function assertEncodeValues($type, $val, $bits): void
    {
        if (self::FLOAT_TYPE === $type) {
            $decoder = [IOBinaryDecoder::class, 'intBitsToFloat'];
            $encoder = [IOBinaryEncoder::class, 'floatToIntBits'];
        } else {
            $decoder = [IOBinaryDecoder::class, 'longBitsToDouble'];
            $encoder = [IOBinaryEncoder::class, 'doubleToLongBits'];
        }

        $decodedBitsValue = $decoder($bits);
        $this->assertEquals(
            $val,
            $decodedBitsValue,
            sprintf(
                "%s\n expected: '%f'\n    given: '%f'",
                'DECODED BITS',
                $val,
                $decodedBitsValue
            )
        );

        $encodedValueBits = $encoder($val);
        $this->assertEquals(
            $bits,
            $encodedValueBits,
            sprintf(
                "%s\n expected: '%s'\n    given: '%s'",
                'ENCODED VAL',
                Debug::hexString($bits),
                Debug::hexString($encodedValueBits)
            )
        );

        $roundTripValue = $decoder($encodedValueBits);
        $this->assertEquals(
            $val,
            $roundTripValue,
            sprintf(
                "%s\n expected: '%f'\n     given: '%f'",
                'ROUND TRIP BITS',
                $val,
                $roundTripValue
            )
        );
    }

    private function assertEncodeNanValues($type, $val, $bits): void
    {
        if (self::FLOAT_TYPE === $type) {
            $decoder = [IOBinaryDecoder::class, 'intBitsToFloat'];
            $encoder = [IOBinaryEncoder::class, 'floatToIntBits'];
        } else {
            $decoder = [IOBinaryDecoder::class, 'longBitsToDouble'];
            $encoder = [IOBinaryEncoder::class, 'doubleToLongBits'];
        }

        $decodedBitsValue = $decoder($bits);
        $this->assertTrue(
            is_nan($decodedBitsValue),
            sprintf(
                "%s\n expected: '%f'\n    given: '%f'",
                'DECODED BITS',
                $val,
                $decodedBitsValue
            )
        );

        $encodedValueBits = $encoder($val);
        $this->assertEquals(
            $bits,
            $encodedValueBits,
            sprintf(
                "%s\n expected: '%s'\n    given: '%s'",
                'ENCODED VAL',
                Debug::hexString($bits),
                Debug::hexString($encodedValueBits)
            )
        );

        $roundTripValue = $decoder($encodedValueBits);
        $this->assertTrue(
            is_nan($roundTripValue),
            sprintf(
                "%s\n expected: '%f'\n     given: '%f'",
                'ROUND TRIP BITS',
                $val,
                $roundTripValue
            )
        );
    }

    private function normalValuesProvider(): iterable
    {
        yield [self::DOUBLE_TYPE, (float) -10, "\000\000\000\000\000\000$\300", '000000000000420c'];
        yield [self::DOUBLE_TYPE, (float) -9, "\000\000\000\000\000\000\"\300", '000000000000220c'];
        yield [self::DOUBLE_TYPE, (float) -8, "\000\000\000\000\000\000 \300", '000000000000020c'];
        yield [self::DOUBLE_TYPE, (float) -7, "\000\000\000\000\000\000\034\300", '000000000000c10c'];
        yield [self::DOUBLE_TYPE, (float) -6, "\000\000\000\000\000\000\030\300", '000000000000810c'];
        yield [self::DOUBLE_TYPE, (float) -5, "\000\000\000\000\000\000\024\300", '000000000000410c'];
        yield [self::DOUBLE_TYPE, (float) -4, "\000\000\000\000\000\000\020\300", '000000000000010c'];

        yield [self::DOUBLE_TYPE, (float) -3, "\000\000\000\000\000\000\010\300", '000000000000800c'];
        yield [self::DOUBLE_TYPE, (float) -2, "\000\000\000\000\000\000\000\300", '000000000000000c'];
        yield [self::DOUBLE_TYPE, (float) -1, "\000\000\000\000\000\000\360\277", '0000000000000ffb'];
        yield [self::DOUBLE_TYPE, (float) 0, "\000\000\000\000\000\000\000\000", '0000000000000000'];
        yield [self::DOUBLE_TYPE, (float) 1, "\000\000\000\000\000\000\360?", '0000000000000ff3'];
        yield [self::DOUBLE_TYPE, (float) 2, "\000\000\000\000\000\000\000@", '0000000000000004'];

        yield [self::DOUBLE_TYPE, (float) 3, "\000\000\000\000\000\000\010@", '0000000000008004'];
        yield [self::DOUBLE_TYPE, (float) 4, "\000\000\000\000\000\000\020@", '0000000000000104'];
        yield [self::DOUBLE_TYPE, (float) 5, "\000\000\000\000\000\000\024@", '0000000000004104'];
        yield [self::DOUBLE_TYPE, (float) 6, "\000\000\000\000\000\000\030@", '0000000000008104'];
        yield [self::DOUBLE_TYPE, (float) 7, "\000\000\000\000\000\000\034@", '000000000000c104'];
        yield [self::DOUBLE_TYPE, (float) 8, "\000\000\000\000\000\000 @", '0000000000000204'];
        yield [self::DOUBLE_TYPE, (float) 9, "\000\000\000\000\000\000\"@", '0000000000002204'];
        yield [self::DOUBLE_TYPE, (float) 10, "\000\000\000\000\000\000$@", '0000000000004204'];

        yield [self::DOUBLE_TYPE, (float) -1234.2132, "\007\316\031Q\332H\223\300", '70ec9115ad84390c'];
        yield [self::DOUBLE_TYPE, (float) -2.11e+25, "\311\260\276J\031t1\305", '9c0beba49147135c'];

        yield [self::FLOAT_TYPE, (float) -10, "\000\000 \301", '0000021c'];
        yield [self::FLOAT_TYPE, (float) -9, "\000\000\020\301", '0000011c'];
        yield [self::FLOAT_TYPE, (float) -8, "\000\000\000\301", '0000001c'];
        yield [self::FLOAT_TYPE, (float) -7, "\000\000\340\300", '00000e0c'];
        yield [self::FLOAT_TYPE, (float) -6, "\000\000\300\300", '00000c0c'];
        yield [self::FLOAT_TYPE, (float) -5, "\000\000\240\300", '00000a0c'];
        yield [self::FLOAT_TYPE, (float) -4, "\000\000\200\300", '0000080c'];
        yield [self::FLOAT_TYPE, (float) -3, "\000\000@\300", '0000040c'];
        yield [self::FLOAT_TYPE, (float) -2, "\000\000\000\300", '0000000c'];
        yield [self::FLOAT_TYPE, (float) -1, "\000\000\200\277", '000008fb'];
        yield [self::FLOAT_TYPE, (float) 0, "\000\000\000\000", '00000000'];
        yield [self::FLOAT_TYPE, (float) 1, "\000\000\200?", '000008f3'];
        yield [self::FLOAT_TYPE, (float) 2, "\000\000\000@", '00000004'];
        yield [self::FLOAT_TYPE, (float) 3, "\000\000@@", '00000404'];
        yield [self::FLOAT_TYPE, (float) 4, "\000\000\200@", '00000804'];
        yield [self::FLOAT_TYPE, (float) 5, "\000\000\240@", '00000a04'];
        yield [self::FLOAT_TYPE, (float) 6, "\000\000\300@", '00000c04'];
        yield [self::FLOAT_TYPE, (float) 7, "\000\000\340@", '00000e04'];
        yield [self::FLOAT_TYPE, (float) 8, "\000\000\000A", '00000014'];
        yield [self::FLOAT_TYPE, (float) 9, "\000\000\020A", '00000114'];
        yield [self::FLOAT_TYPE, (float) 10, "\000\000 A", '00000214'];
        yield [self::FLOAT_TYPE, (float) -1234.5, "\000P\232\304", '0005a94c'];
        yield [self::FLOAT_TYPE, (float) -211300000.0, "\352\202I\315", 'ae2894dc'];
    }
}
