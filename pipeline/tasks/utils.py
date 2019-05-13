import luigi
from luigi.contrib import ExternalProgramTask

class MetaOutputHandler:
    def output(self):
        return {key: task for key,task in self.input().items()}

class Wget(ExternalProgramTask):
    url = luigi.Parameter()
    output_file = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def program_args(self):
        return ['wget', '-O', self.output().path, self.url]
