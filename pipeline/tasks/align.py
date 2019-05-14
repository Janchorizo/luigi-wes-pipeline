import luigi
from luigi.contrib.external_program import ExternalProgramTask
from os import path
from tasks.utils import MetaOutputHandler
from tasks.utils import Wget
from tasks.utils import GlobalParams

from tasks.reference import ReferenceGenome
from tasks.fastq import GetFastq

class BwaAlignFastq(ExternalProgramTask):
    def requires(self): 
        return {
            'reference' : ReferenceGenome(),
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
            self.input()['reference']['fa']['fa'].path,
            self.input()['fastq']['fastq1'].path,
            ]

        if 'fastq_2' in self.input()['fastq']:
            args.append(self.input()['fastq']['fastq2'].path)

        args.append('-o')
        args.append(self.output().path)

        return args

class FastqAlign(MetaOutputHandler, luigi.WrapperTask):
    create_report = luigi.Parameter(default='')
    cpus = luigi.Parameter(default='')

    def requires(self):
        return {
            'sam' : BwaAlignFastq()
            }

if __name__ == '__main__':
    luigi.run(['FastqAlign', 
            '--FastqAlign-create-report', 'False',
            '--FastqAlign-cpus', '8',
            '--GetFastq-fastq1-url', '',
            '--GetFastq-fastq2-url', '',
            '--GetFastq-from-ebi', 'False',
            '--GetFastq-paired-end', 'False',
            '--ReferenceGenome-ref-url', 'ftp://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit',
            '--ReferenceGenome-from2bit', 'True',
            '--GlobalParams-base-dir', path.abspath('./experiment'),
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'hg19'])
