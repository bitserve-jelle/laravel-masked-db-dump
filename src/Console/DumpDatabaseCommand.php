<?php

namespace BeyondCode\LaravelMaskedDumper\Console;

use Generator;
use Illuminate\Console\Command;
use BeyondCode\LaravelMaskedDumper\LaravelMaskedDump;

class DumpDatabaseCommand extends Command
{
    protected $signature = 'db:masked-dump {output} {--definition=default} {--gzip}';

    protected $description = 'Create a new database dump';

    public function handle()
    {
        if (file_exists($filename = $this->filename())) {
            $this->error("Output file already exists: $filename");

            return 1;
        }

        $definition = config('masked-dump.' . $this->option('definition'));

        // https://github.com/beyondcode/laravel-masked-db-dump/pull/15/commits/216f78933d0ae55b719434726816234227acf5ae
        $definition = is_callable($definition) ? call_user_func($definition) : $definition;

        $definition->load();

        $this->info('Starting Database dump');

        $dumper = new LaravelMaskedDump($definition, $this->output);
        $generator = $dumper->dump();

        $this->writeOutput($generator);
    }

    protected function writeOutput(Generator $dump)
    {
        try {
            $handle = $this->openFile();

            foreach ($dump as $output) {
                $this->writeLine($handle, $output);
            }
        } finally {
            $this->closeFile($handle);
        }

        $this->newLine();
        $this->info('Wrote database dump to ' . $this->filename());
    }

    private function filename(): string
    {
        $filename = $this->argument('output');

        if ($this->option('gzip')) {
            $filename .= '.gz';
        }

        return $filename;
    }

    private function openFile()
    {
        if ($this->option('gzip')) {
            return gzopen($this->filename(), 'w9');
        }

        return fopen($this->filename(), 'w');
    }

    private function writeLine($handle, string $output): void
    {
        if ($this->option('gzip')) {
            gzwrite($handle, $output);

            return;
        }

        fwrite($handle, $output);
    }

    private function closeFile($handle): void
    {
        if ($this->option('gzip')) {
            gzclose($handle);

            return;
        }

        fclose($handle);
    }
}
