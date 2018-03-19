<?php

namespace Avro;

use Avro\Debug\Debug;
use Avro\Exception\Exception;

class Avro
{
    // Version number of Avro specification to which this implementation complies.
    public const SPEC_VERSION = '1.3.3';

    // Constant to enumerate endianness.
    private const BIG_ENDIAN = 0x00;
    private const LITTLE_ENDIAN = 0x01;

    // Constant to enumerate biginteger handling mode. GMP is used, if available, on 32-bit platforms.
    private const PHP_BIGINT_MODE = 0x00;
    private const GMP_BIGINT_MODE = 0x01;

    /**
     * Memorized result of self::setEndianness().
     *
     * @var int
     */
    private static $endianness;

    /**
     * Mode used to handle big integers. After self::check64Bit() has been called,
     * (usually via a call to self::checkPlatform(), set to self::GMP_BIGINT_MODE on 32-bit platforms
     * that have GMP available, and to self::PHP_BIGINT_MODE otherwise.
     *
     * @var int
     */
    private static $bigintMode;

    /**
     * Wrapper method to call each required check.
     */
    public static function checkPlatform(): void
    {
        self::check64Bit();
        self::checkLittleEndian();
    }

    /**
     * Checked if the PHP GMP extension is used.
     *
     * @internal requires self::check64Bit() (exposed via self::checkPlatform()) to have been called
     * to set self::$bigintMode
     */
    public static function usesGmp(): bool
    {
        return self::GMP_BIGINT_MODE === self::$bigintMode;
    }

    /**
     * Determines if the host platform can encode and decode long integer data.
     */
    private static function check64Bit(): void
    {
        if (8 !== PHP_INT_SIZE) {
            if (extension_loaded('gmp')) {
                self::$bigintMode = self::GMP_BIGINT_MODE;
            } else {
                throw new Exception(
                    'This platform cannot handle a 64-bit operations. Please install the GMP PHP extension.'
                );
            }
        } else {
            self::$bigintMode = self::PHP_BIGINT_MODE;
        }
    }

    /**
     * Determines if the host platform is little endian, required for processing double and float data.
     */
    private static function checkLittleEndian(): void
    {
        if (!self::isLittleEndianPlatform()) {
            throw new Exception('This is not a little-endian platform');
        }
    }

    /**
     * Determines the endianness of the host platform and memoizes the result to self::$endianness.
     * Based on a similar check perfomed in http://pear.php.net/package/Math_BinaryUtils.
     */
    private static function setEndianness(): void
    {
        $packed = pack('d', 1);
        switch ($packed) {
            case "\77\360\0\0\0\0\0\0":
                self::$endianness = self::BIG_ENDIAN;
                break;
            case "\0\0\0\0\0\0\360\77":
                self::$endianness = self::LITTLE_ENDIAN;
                break;
            default:
                throw new Exception(sprintf('Error determining platform endianness: %s', Debug::hexString($packed)));
        }
    }

    private static function isBigEndianPlatform(): bool
    {
        if (null === self::$endianness) {
            self::setEndianness();
        }

        return self::BIG_ENDIAN === self::$endianness;
    }

    private static function isLittleEndianPlatform(): bool
    {
        return !self::isBigEndianPlatform();
    }
}
