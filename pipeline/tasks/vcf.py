import luigi
from luigi.contrib.external_program import ExternalProgramTask
from os import path
from .utils import MetaOutputHandler
from .utils import Wget
from .utils import GlobalParams

from .reference import ReferenceGenome
from .process_align import AlignProcessing

class PlatypusCallVariants(ExternalProgramTask):
    def requires(self):
        return {
            'reference' : ReferenceGenome(),
            'process' : AlignProcessing()
        }

    def output(self):
        return luigi.LocalTarget(
            path.join(GlobalParams().base_dir,
            GlobalParams().exp_name+'.vcf'))

    def program_args(self):
        return ['platypus', 'callVariants', 
            '--bamFiles='+self.input()['process']['bamNoDup']['bam'].path, 
            '--refFile='+self.input()['reference']['fa'].path, 
            '--output='+self.output().path,
        ]

class Vcf(MetaOutputHandler, luigi.WrapperTask):
    def requires(self):
        return PlatypusCallVariants()

if __name__ == '__main__':
    luigi.run(['Vcf', 
            '--GlobalParams-base-dir', path.abspath(path.curdir),
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'getfastaq'])
