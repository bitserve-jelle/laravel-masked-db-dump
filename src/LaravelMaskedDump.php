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

        yield 'SET AUTOCOMMIT = 0;' . PHP_EOL
            . 'SET UNIQUE_CHECKS = 0;' . PHP_EOL
            . 'SET FOREIGN_KEY_CHECKS = 0;' . PHP_EOL;

        $overallTableProgress = $this->output->createProgressBar(count($tables));
        $overallTableProgress->setFormat("[%current%/%max%] <info>Exporting table...</info> <comment>%table%</comment>");

        foreach ($tables as $tableName => $table) {
            $overallTableProgress->setMessage($tableName, 'table');
            $overallTableProgress->display();

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

        $overallTableProgress->setFormat("[%current%/%max%] <info>All tables exported!</info>");
        $overallTableProgress->display();

        yield 'SET FOREIGN_KEY_CHECKS = 1;' . PHP_EOL
            . 'SET UNIQUE_CHECKS = 1;' . PHP_EOL
            . 'SET AUTOCOMMIT = 1;' . PHP_EOL;
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

        $chunks = $queryBuilder->lazyById()->chunk(5);

        foreach ($chunks as $rows) {
            $tableName = $table->getDoctrineTable()->getName();

            $row = $rows->first();
            $columnNames = array_map(
                function (string $column) {
                    return "`$column`";
                },
                array_keys((array)$row),
            );

            $values = $rows->map(function ($row) use ($table) {
                $row = $this->transformResultForInsert((array)$row, $table);

                return '(' . join(', ', $row) . ')';
            })
                ->join(', ');

            $statement = sprintf(
                'INSERT INTO `%s` (%s) VALUES %s;',
                $tableName,
                join(', ', $columnNames),
                $values,
            );

            yield $statement . PHP_EOL;
        }
    }
}
