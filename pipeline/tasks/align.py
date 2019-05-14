import luigi
from luigi.contrib.external_program import ExternalProgramTask
from os import path
from .utils import MetaOutputHandler
from .utils import Wget
from .utils import GlobalParams

from .reference import ReferenceGenome
from .fastq import Fastq

class BwaAlignFastq(ExternalProgramTask):
    def requires(self): 
        return {
            'reference' : GetReference(),
            'fastq' : GetFastq()
        }

    def output(self):
        return luigi.LocalTarget(
            path.join(GlobalParams().base_dir,
            GlobalParams().exp_name+".sam"))

    def program_args(self):
        name = GlobalParams().exp_name
        rg = '@RG\\tID:'+name+'\\tSM:'+name+'\\tPL:illumina'

        args = ['bwa', 'mem', '-M', '-R', rg,
            '-t', FastqAlign().cpus,
            self.input()['reference']['fa'].path,
            self.input()['fastq']['fastq1'].path,
            ]

        if 'fastq2' in self.input()['fastq']:
            args.push(self.input()['fastq']['fastq2'].path)

        args.push('-o')
        args.push(self.output().path)

        return args

class FastqAlign(MetaOutputHandler, luigi.WrapperTask):
    create_report = luigi.Parameter(default='')
    cpus = luigi.Parameter()

    def requires(self):
        return {
            'sam' : BwaAlignFastq()
            }

if __name__ == '__main__':
    luigi.run(['FastqAlign',
            '--FastqAlign-cpus', '6', 
            '--FastqAlign-create-report', 'True', 
            '--GlobalParams-base-dir', path.abspath(path.curdir),
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'get_ref_genome'])
