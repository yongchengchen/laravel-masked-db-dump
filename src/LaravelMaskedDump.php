<?php

namespace BeyondCode\LaravelMaskedDumper;

use BeyondCode\LaravelMaskedDumper\TableDefinitions\TableDefinition;
use Closure;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\MySqlPlatform;
use Doctrine\DBAL\Schema\Schema;
use Illuminate\Console\OutputStyle;

class LaravelMaskedDump
{
    /** @var DumpSchema */
    protected $definition;

    /** @var OutputStyle */
    protected $output;

    protected $firstRow;
    protected $useMysqldump;

    public function __construct(DumpSchema $definition, OutputStyle $output)
    {
        $this->useMysqldump = true; //env('USE_MYSQLDUMP')
        $this->definition = $definition;
        $this->output = $output;

        if ($this->useMysqldump) {
            $platform = $this->definition->getConnection()->getDoctrineSchemaManager()->getDatabasePlatform();
            $this->useMysqldump = $platform instanceof MysqlPlatform;
        }
    }

    public function dump(Closure $writer)
    {
        $tables = $this->definition->getDumpTables();

        $overallTableProgress = $this->output->createProgressBar(count($tables));

        if ($this->useMysqldump) {
            $this->dumpDBSchemaViaMysqldump($writer);
        }

        $writer('SET FOREIGN_KEY_CHECKS=0;');

        foreach ($tables as $tableName => $table) {
            if (!$this->useMysqldump) {
                $writer("DROP TABLE IF EXISTS `$tableName`;" . PHP_EOL);
                $writer($this->dumpSchema($table));
            }

            if ($table->shouldDumpData()) {
                $writer($this->lockTable($tableName));

                $this->dumpTableData($table, $writer);

                $writer($this->unlockTable($tableName));
            }

            $overallTableProgress->advance();
        }

        $writer('SET FOREIGN_KEY_CHECKS=1;');
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
        $tableName = $table->getDoctrineTable()->getName();
        if ($queryBuilder->exists()) {
            $writer("INSERT INTO `${tableName}` VALUES");
            $this->firstRow = true;
            $queryBuilder->chunkById(500, function ($chunk) use ($table, $writer) {
                foreach ($chunk as $item) {
                    $row = $this->transformResultForInsert((array) $item, $table);
                    if (!$this->firstRow) {
                        $writer(",");
                    } else {
                        $this->firstRow = false;
                    }
                    $writer("(");
                    $writer(implode(',', array_values($row)));
                    $writer(")");
                }
            }, $this->getPrimary($table->getDoctrineTable()));

            $writer(';' . PHP_EOL);
        }
    }

    protected function getPrimary($table)
    {
        if ($table->hasIndex('primary')) {
            return $table->getIndex('primary')->getColumns()[0];
        }

        if ($columns = $table->getColumns()) {
            foreach ($columns as $name => $col) {
                return $name;
            }
        }
    }

    public function dumpDBSchemaViaMysqldump($writer)
    {
        $configs = $this->definition->getConnection()->getConfig();
        $host = $configs['host'] ?? '';
        $database = $configs['database'] ?? '';
        $username = $configs['username'] ?? '';
        $password = $configs['password'] ?? '';

        $dir = sprintf('/tmp/%s.schema.sql', $database);
        exec("mysqldump --user={$username} --password={$password} --host={$host} {$database} --no-data --lock-tables=false --result-file={$dir} 2>&1", $output);

        $writer(file_get_contents($dir));
        unlink($dir);
    }
}
