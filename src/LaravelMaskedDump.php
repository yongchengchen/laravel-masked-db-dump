<?php

namespace BeyondCode\LaravelMaskedDumper;

use BeyondCode\LaravelMaskedDumper\TableDefinitions\TableDefinition;
use Closure;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Illuminate\Console\OutputStyle;

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

    public function dump(Closure $writer)
    {
        $tables = $this->definition->getDumpTables();

        $overallTableProgress = $this->output->createProgressBar(count($tables));

        foreach ($tables as $tableName => $table) {
            $writer("DROP TABLE IF EXISTS `$tableName`;" . PHP_EOL);
            $writer($this->dumpSchema($table));

            if ($table->shouldDumpData()) {
                $writer($this->lockTable($tableName));

                $this->dumpTableData($table, $writer);

                $writer($this->unlockTable($tableName));
            }

            $overallTableProgress->advance();
        }
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

    protected function dumpTableData(TableDefinition $table, Closure $writer)
    {

        $queryBuilder = $this->definition->getConnection()
            ->table($table->getDoctrineTable()->getName());

        $table->modifyQuery($queryBuilder);

        $queryBuilder->get()
            ->each(function ($item, $index) use ($table, $writer) {
                $row = $this->transformResultForInsert((array) $item, $table);
                unset($item);
                $tableName = $table->getDoctrineTable()->getName();

                $writer("INSERT INTO `${tableName}` (`" . implode('`, `', array_keys($row)) . '`) VALUES (');

                $firstColumn = true;
                foreach ($row as $value) {
                    if (!$firstColumn) {
                        $writer(", ");
                    }
                    $writer($value);
                    $firstColumn = false;
                }
                $writer(");" . PHP_EOL);

                unset($row);
            });
    }
}
