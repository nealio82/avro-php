<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class Name
{
    private const NAME_SEPARATOR = '.';
    private const NAME_REGEXP = '/^[A-Za-z_][\w]*$/';

    private $fullname;
    private $name;
    private $namespace;
    private $qualifiedName;

    public function __construct(string $name, ?string $namespace, ?string $defaultNamespace)
    {
        if (empty($name)) {
            throw new SchemaParseException('Name must be a non-empty string.');
        }

        $this->fullname = $this->extractFullname($name, $namespace, $defaultNamespace);
        $this->name = self::extractName($this->fullname);
        $this->namespace = self::extractNamespace($this->fullname);
        $this->qualifiedName = $this->extractQualifiedName(
            $this->name,
            $this->fullname,
            $this->namespace,
            $defaultNamespace
        );
    }

    public function __toString()
    {
        return $this->fullname;
    }

    public function getFullname(): string
    {
        return $this->fullname;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getNamespace(): ?string
    {
        return $this->namespace;
    }

    public function getQualifiedName(): string
    {
        return $this->qualifiedName;
    }

    public static function extractName(string $name): string
    {
        $parts = explode(self::NAME_SEPARATOR, $name);
        if (count($parts) > 1) {
            $name = array_pop($parts);
        }

        return $name;
    }

    public static function extractNamespace(string $name, ?string $namespace = null): ?string
    {
        $parts = explode(self::NAME_SEPARATOR, $name);
        if (count($parts) > 1) {
            array_pop($parts);
            $namespace = implode(self::NAME_SEPARATOR, $parts);
        }

        return $namespace;
    }

    public static function isWellFormedName(string $name): bool
    {
        return preg_match(self::NAME_REGEXP, $name);
    }

    private function extractFullname(string $name, ?string $namespace, ?string $defaultNamespace): string
    {
        if (strpos($name, self::NAME_SEPARATOR) && $this->checkNamespaceNames($name)) {
            return $name;
        }

        if (!self::isWellFormedName($name)) {
            throw new SchemaParseException(sprintf('Invalid name "%s"', $name));
        }

        if (null !== $namespace) {
            return $this->parseFullname($name, $namespace);
        }

        if (null !== $defaultNamespace) {
            return $this->parseFullname($name, $defaultNamespace);
        }

        return $name;
    }

    private function extractQualifiedName(
        string $name,
        string $fullname,
        ?string $namespace,
        ?string $defaultNamespace
    ): string {
        if (null === $namespace || $namespace === $defaultNamespace) {
            return $name;
        }

        return $fullname;
    }

    private function parseFullname(string $name, string $namespace): string
    {
        if ('' === $namespace) {
            throw new SchemaParseException('Namespace must be a non-empty string.');
        }
        $this->checkNamespaceNames($namespace);

        return $namespace.'.'.$name;
    }

    private function checkNamespaceNames(string $namespace): bool
    {
        foreach (explode(self::NAME_SEPARATOR, $namespace) as $namespacePart) {
            if (!self::isWellFormedName($namespacePart)) {
                throw new SchemaParseException(sprintf('Invalid name "%s"', $namespacePart));
            }
        }

        return true;
    }
}
