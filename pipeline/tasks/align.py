import luigi
from luigi.contrib.external_program import ExternalProgramTask
from os import path
from .utils import MetaOutputHandler
from .utils import Wget
from .utils import GlobalParams

from .reference import GetReference
from .fastq import GetFastq

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
            '-t', GlobalParams().cpus,
            self.input()['reference']['fa'].path,
            self.input()['fastq']['fastq_1'].path,
            ]

        if 'fastq_2' in self.input()['fastq']:
            args.push(self.input()['fastq']['fastq_2'].path)

        args.push('-o')
        args.push(self.output().path)

        return args

class FastqAlign(MetaOutputHandler, luigi.WrapperTask):
    create_inform = luigi.Parameter(default='')

    def requires(self):
        return {
            'sam' : BwaAlignFastq()
            }

if __name__ == '__main__':
    luigi.run(['ReferenceGenome', 
            '--ReferenceGenome-ref-url', 'ftp://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit',
            '--ReferenceGenome-from2bit', 'True',
            '--GlobalParams-base-dir', path.abspath(path.curdir),
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'get_ref_genome'])
