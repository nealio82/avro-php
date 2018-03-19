<?php

namespace Avro\GMP;

/**
 * Methods for handling 64-bit operations using the GMP extension.
 *
 * This is a naive and hackish implementation that is intended to work well enough to support Avro.
 */
class GMP
{
    /**
     * @var \GMP|resource|string|null memoized GMP resource for zero
     */
    private static $gmp0;

    /**
     * @var \GMP|resource|string|null memoized GMP resource for one (1)
     */
    private static $gmp1;

    /**
     * @var \GMP|resource|string|null memoized GMP resource for two (2)
     */
    private static $gmp2;

    /**
     * @var \GMP|resource|string|null memoized GMP resource for 0x7f
     */
    private static $gmp0x7f;

    /**
     * @var \GMP|resource|string|null memoized GMP resource for 64-bit ~0x7f
     */
    private static $gmpN0x7f;

    /**
     * @var \GMP|resource|string|null memoized GMP resource for 64-bits of 1
     */
    private static $gmp0xfs;

    /**
     * @param \GMP|resource|string $gmp
     *
     * @return \GMP resource 64-bit two's complement of input
     */
    public static function gmpTwosComplement($gmp): \GMP
    {
        return gmp_neg(gmp_sub(gmp_pow(self::gmp2(), 64), $gmp));
    }

    /**
     * @internal only works up to shift 63 (doesn't wrap bits around)
     *
     * @param \GMP|resource|string $gmp
     */
    public static function shiftLeft($gmp, int $shift): string
    {
        if (0 === $shift) {
            return $gmp;
        }

        if (gmp_sign($gmp) < 0) {
            $gmp = self::gmpTwosComplement($gmp);
        }

        $matches = gmp_mul($gmp, gmp_pow(self::gmp2(), $shift));
        $matches = gmp_and($matches, self::gmp0xfs());
        if (gmp_testbit($matches, 63)) {
            $matches = gmp_neg(gmp_add(gmp_and(gmp_com($matches), self::gmp0xfs()), self::gmp1()));
        }

        return $matches;
    }

    /**
     * @param \GMP|resource|string $gmp
     */
    public static function shiftRight($gmp, int $shift): string
    {
        if (0 === $shift) {
            return $gmp;
        }

        if (0 <= gmp_sign($gmp)) {
            $matches = gmp_div($gmp, gmp_pow(self::gmp2(), $shift));
        } else { // negative
            $gmp = gmp_and($gmp, self::gmp0xfs());
            $matches = gmp_div($gmp, gmp_pow(self::gmp2(), $shift));
            $matches = gmp_and($matches, self::gmp0xfs());
            for ($i = 63; $i >= (63 - $shift); --$i) {
                gmp_setbit($matches, $i);
            }

            $matches = gmp_neg(gmp_add(gmp_and(gmp_com($matches), self::gmp0xfs()), self::gmp1()));
        }

        return $matches;
    }

    /**
     * @param int|string $number integer (or string representation of integer) to encode
     *
     * @return string $bytes of the long $n encoded per the Avro spec
     */
    public static function encodeLong($number): string
    {
        $gmp = gmp_init($number);
        $gmp = gmp_xor(self::shiftLeft($gmp, 1), self::shiftRight($gmp, 63));
        $bytes = '';
        while (0 !== gmp_cmp(self::gmp0(), gmp_and($gmp, self::gmpN0x7f()))) {
            $bytes .= chr(gmp_intval(gmp_and($gmp, self::gmp0x7f())) | 0x80);
            $gmp = self::shiftRight($gmp, 7);
        }
        $bytes .= chr(gmp_intval($gmp));

        return $bytes;
    }

    /**
     * @param int[] $bytes array of ascii codes of bytes to decode
     */
    public static function decodeLongFromArray(array $bytes): string
    {
        $byte = array_shift($bytes);
        $gmp = gmp_init($byte & 0x7f);
        $shift = 7;
        while (0 !== ($byte & 0x80)) {
            $byte = array_shift($bytes);
            $gmp = gmp_or($gmp, self::shiftLeft($byte & 0x7f, $shift));
            $shift += 7;
        }
        $value = gmp_xor(self::shiftRight($gmp, 1), gmp_neg(gmp_and($gmp, 1)));

        return gmp_strval($value);
    }

    /**
     * Returns GMP resource for zero.
     */
    private static function gmp0(): \GMP
    {
        if (null === self::$gmp0) {
            self::$gmp0 = gmp_init('0');
        }

        return self::$gmp0;
    }

    /**
     * Returns GMP resource for one (1).
     */
    private static function gmp1(): \GMP
    {
        if (null === self::$gmp1) {
            self::$gmp1 = gmp_init('1');
        }

        return self::$gmp1;
    }

    /**
     * Returns GMP resource for two (2).
     */
    private static function gmp2(): \GMP
    {
        if (null === self::$gmp2) {
            self::$gmp2 = gmp_init('2');
        }

        return self::$gmp2;
    }

    /**
     * Returns GMP resource for 0x7f.
     */
    private static function gmp0x7f(): \GMP
    {
        if (null === self::$gmp0x7f) {
            self::$gmp0x7f = gmp_init('0x7f');
        }

        return self::$gmp0x7f;
    }

    /**
     * Returns GMP resource for 64-bit ~0x7f.
     */
    private static function gmpN0x7f(): \GMP
    {
        if (null === self::$gmpN0x7f) {
            self::$gmpN0x7f = gmp_init('0xffffffffffffff80');
        }

        return self::$gmpN0x7f;
    }

    /**
     * Returns GMP resource for 64-bits of 1.
     */
    private static function gmp0xfs(): \GMP
    {
        if (null === self::$gmp0xfs) {
            self::$gmp0xfs = gmp_init('0xffffffffffffffff');
        }

        return self::$gmp0xfs;
    }
}
