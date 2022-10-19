<?php

namespace BeyondCode\LaravelMaskedDumper\TableDefinitions;

use BeyondCode\LaravelMaskedDumper\Contracts\Column;
use BeyondCode\LaravelMaskedDumper\ColumnDefinitions\ColumnDefinition;
use Doctrine\DBAL\Schema\Table;

class TableDefinition
{
    const DUMP_FULL = 'full';
    const DUMP_SCHEMA = 'schema';
    const DUMP_PARTIAL = 'partial';

    protected $table;
    protected $dumpType;
    protected $query;
    protected $columns = [];

    private $extraDumpSql;

    public function __construct(Table $table)
    {
        $this->table = $table;
        $this->dumpType = static::DUMP_FULL;
    }

    public function schemaOnly()
    {
        $this->dumpType = static::DUMP_SCHEMA;

        return $this;
    }

    public function fullDump()
    {
        $this->dumpType = static::DUMP_FULL;

        return $this;
    }

    public function partialDump()
    {
        $this->dumpType = static::DUMP_PARTIAL;

        return $this;
    }

    public function query(callable $callable)
    {
        $this->query = $callable;
    }

    public function dumpSql(callable $callable)
    {
        $this->extraDumpSql = $callable;
    }

    public function mask(string $column, string $maskCharacter = 'x')
    {
        $this->columns[$column] = ColumnDefinition::mask($column, $maskCharacter);

        return $this;
    }

    public function replace(string $column, $replacer)
    {
        $this->columns[$column] = ColumnDefinition::replace($column, $replacer);

        return $this;
    }

    /**
     * @param string $column
     * @return Column|null
     */
    public function findColumn(string $column)
    {
        if (array_key_exists($column, $this->columns)) {
            return $this->columns[$column];
        }

        return false;
    }

    public function getDoctrineTable()
    {
        return $this->table;
    }

    public function shouldDumpData()
    {
        return $this->dumpType === static::DUMP_FULL
            || $this->dumpType === static::DUMP_PARTIAL;
    }

    public function shouldRecreateTable()
    {
        return $this->dumpType !== static::DUMP_PARTIAL;
    }

    public function modifyQuery($query)
    {
        if (is_null($this->query)) {
            return;
        }
        call_user_func($this->query, $query);
    }

    public function extraDumpSql(): string
    {
        if (is_null($this->extraDumpSql)) {
            return '';
        }

        return call_user_func($this->extraDumpSql);
    }
}
