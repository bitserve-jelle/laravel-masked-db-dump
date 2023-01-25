<?php

namespace BeyondCode\LaravelMaskedDumper;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Generator;
use Illuminate\Console\OutputStyle;
use BeyondCode\LaravelMaskedDumper\TableDefinitions\TableDefinition;

class LaravelMaskedDump
{
    /** @var DumpSchema */
    protected $definition;

    /** @var OutputStyle */
    protected $output;

    public function __construct(DumpSchema $definition, OutputStyle $output)
    {
        $this->definition = $definition;
        $this->output = $output;
    }

    public function dump(): Generator
    {
        $tables = $this->definition->getDumpTables();

        yield 'SET FOREIGN_KEY_CHECKS = 0;' . PHP_EOL;

        $overallTableProgress = $this->output->createProgressBar(count($tables));

        foreach ($tables as $tableName => $table) {
            if ($table->shouldRecreateTable()) {
                yield "DROP TABLE IF EXISTS `$tableName`;" . PHP_EOL;

                yield $this->dumpSchema($table);
            }

            if ($table->shouldDumpData()) {
                yield $this->lockTable($tableName);

                yield $table->extraDumpSql();

                foreach ($this->dumpTableData($table) as $statement) {
                    yield $statement;
                }

                yield $this->unlockTable($tableName);
            }

            $overallTableProgress->advance();
        }

        yield 'SET FOREIGN_KEY_CHECKS = 1;' . PHP_EOL;
    }

    protected function transformResultForInsert($row, TableDefinition $table)
    {
        /** @var Connection $connection */
        $connection = $this->definition->getConnection()->getDoctrineConnection();

        return collect($row)->map(function ($value, $column) use ($connection, $table) {
            if ($columnDefinition = $table->findColumn($column)) {
                $value = $columnDefinition->modifyValue($value);
            }

            if ($value === null) {
                return 'NULL';
            }
            if ($value === '') {
                return '""';
            }

            return $connection->quote($value);
        })->toArray();
    }

    protected function dumpSchema(TableDefinition $table)
    {
        $platform = $this->definition->getConnection()->getDoctrineSchemaManager()->getDatabasePlatform();

        $schema = new Schema([$table->getDoctrineTable()]);

        return implode(";", $schema->toSql($platform)) . ";" . PHP_EOL;
    }

    protected function lockTable(string $tableName)
    {
        return "LOCK TABLES `$tableName` WRITE;" . PHP_EOL .
            "ALTER TABLE `$tableName` DISABLE KEYS;" . PHP_EOL;
    }

    protected function unlockTable(string $tableName)
    {
        return "ALTER TABLE `$tableName` ENABLE KEYS;" . PHP_EOL .
            "UNLOCK TABLES;" . PHP_EOL;
    }

    protected function dumpTableData(TableDefinition $table): Generator
    {
        $queryBuilder = $this->definition->getConnection()
            ->table($table->getDoctrineTable()->getName());

        $table->modifyQuery($queryBuilder);

        $results = $queryBuilder->lazyById();

        foreach ($results as $result) {
            $row = $this->transformResultForInsert((array)$result, $table);
            $tableName = $table->getDoctrineTable()->getName();

            $query = "INSERT INTO `${tableName}` (`" . implode('`, `', array_keys($row)) . '`) VALUES ';
            $query .= "(";

            $firstColumn = true;
            foreach ($row as $value) {
                if (!$firstColumn) {
                    $query .= ", ";
                }
                $query .= $value;
                $firstColumn = false;
            }

            $query .= ");" . PHP_EOL;

            yield $query;
        }
    }
}
