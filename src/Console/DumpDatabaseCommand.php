<?php

namespace BeyondCode\LaravelMaskedDumper\Console;

use BeyondCode\LaravelMaskedDumper\LaravelMaskedDump;
use Illuminate\Console\Command;
use \Exception;

class DumpDatabaseCommand extends Command
{
    protected $signature = 'db:masked-dump {output} {--definition=default} {--gzip}';

    protected $description = 'Create a new database dump';
    protected $tmpFile;
    protected $pfile;
    protected $gzip;

    public function handle()
    {
        ini_set('memory_limit', '512M');
        $definition = config('masked-dump.' . $this->option('definition'));
        $definition->load();

        $this->info('Starting Database dump');

        $this->prepareWrite();
        $dumper = new LaravelMaskedDump($definition, $this->output);
        try {
            $dumper->dump(function ($content) {
                $this->writeContent($content);
            });
        } catch (Exception $e) {
        }

        $this->finaliseWrite();
    }

    protected function writeOutput(string $dump)
    {
        if ($this->option('gzip')) {
            $gz = gzopen($this->argument('output') . '.gz', 'w9');
            gzwrite($gz, $dump);
            gzclose($gz);

            $this->info('Wrote database dump to ' . $this->argument('output') . '.gz');
        } else {
            file_put_contents($this->argument('output'), $dump);
            $this->info('Wrote database dump to ' . $this->argument('output'));
        }
    }

    protected function prepareWrite()
    {
        if ($this->gzip = $this->option('gzip')) {
            $this->tmpFile = sprintf('/tmp/dbdumper-%s.gz', time());
            $this->pfile = gzopen($this->tmpFile, 'w9');
        } else {
            $this->tmpFile = sprintf('/tmp/dbdumper-%s.sql', time());
            $this->pfile = fopen($this->tmpFile, 'w');
        }
    }

    public function writeContent($str)
    {
        if ($this->gzip) {
            gzwrite($this->pfile, $str);
        } else {
            fwrite($this->pfile, $str);
        }
    }

    protected function finaliseWrite()
    {
        if ($this->pfile) {
            if ($this->gzip) {
                gzclose($this->pfile);
                copy($this->tmpFile, $this->argument('output') . '.gz');
            } else {
                fclose($this->pfile);
                copy($this->tmpFile, $this->argument('output'));
            }
        }

        unlink($this->tmpFile);
    }
}
