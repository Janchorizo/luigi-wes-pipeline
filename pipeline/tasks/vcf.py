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
            GlobalParams().exp_name+'_platypus.vcf'))

    def program_args(self):
        return ['platypus', 'callVariants', 
            '--bamFiles='+self.input()['process']['bamNoDup']['bam'].path, 
            '--refFile='+self.input()['reference']['fa']['fa'].path, 
            '--output='+self.output().path,
        ]

class FreebayesCallVariants(ExternalProgramTask):
    def requires(self):
        return {
            'reference' : ReferenceGenome(),
            'process' : AlignProcessing()
        }

    def output(self):
        return luigi.LocalTarget(
            path.join(GlobalParams().base_dir,
            GlobalParams().exp_name+'_freebayes.vcf'))

    def program_args(self):
        return ['freebayes', 
            '-f', self.input()['reference']['fa']['fa'].path,
            '--bam', self.input()['process']['bamNoDup']['bam'].path,
            '--vcf', self.output().path
        ]

class SamtoolsCallVariants(ExternalProgramTask):
    def requires(self):
        return {
            'reference' : ReferenceGenome(),
            'process' : AlignProcessing()
        }

    def output(self):
        return luigi.LocalTarget(
            path.join(GlobalParams().base_dir,
            GlobalParams().exp_name+'_samtools.vcf'))

    def program_args(self):
        return [
            self.input()['reference']['fa']['fa'].path,
            self.input()['process']['bamNoDup']['bam'].path,
            self.output().path
        ]

class GatkCallVariants(ExternalProgramTask):
    def requires(self):
        return {
            'reference' : ReferenceGenome(),
            'process' : AlignProcessing()
        }

    def output(self):
        return luigi.LocalTarget(
            path.join(GlobalParams().base_dir,
            GlobalParams().exp_name+'_gatk.vcf'))

    def program_args(self):
        return [
            self.input()['reference']['fa']['fa'].path,
            self.input()['process']['bamNoDup']['bam'].path,
            self.output().path
        ]

class DeepcallingCallVariants(ExternalProgramTask):
    def requires(self):
        return {
            'reference' : ReferenceGenome(),
            'process' : AlignProcessing()
        }

    def output(self):
        return luigi.LocalTarget(
            path.join(GlobalParams().base_dir,
            GlobalParams().exp_name+'_deepcalling.vcf'))

    def program_args(self):
        return [
            self.input()['reference']['fa']['fa'].path,
            self.input()['process']['bamNoDup']['bam'].path,
            self.output().path
        ]

class VariantCalling(MetaOutputHandler, luigi.WrapperTask):
    use_platypus = luigi.Parameter(default="")
    use_freebayes = luigi.Parameter(default="")
    use_samtools = luigi.Parameter(default="")
    use_gatk = luigi.Parameter(default="")
    use_deepcalling = luigi.Parameter(default="")

    def requires(self):
        methods = dict()
        if self.use_platypus == 'true':
            methods.update({'platypus': PlatypusCallVariants()})

        if self.use_freebayes == 'true':
            methods.update({'freebayes': FreebayesCallVariants()})

        if self.use_samtools == 'true':
            methods.update({'samtools': SamtoolsCallVariants()})

        if self.use_gatk == 'true':
            methods.update({'gatk': GatkCallVariants()})

        if self.use_deepcalling == 'true':
            methods.update({'deepcalling': DeepcallingCallVariants()})

        return methods


if __name__ == '__main__':
    luigi.run(['VariantCalling', 
            '--VariantCalling-use-platypus', 'true',
            '--VariantCalling-use-freebayes', 'true',
            '--VariantCalling-use-samtools', 'false',
            '--VariantCalling-use-gatk', 'false',
            '--VariantCalling-use-deepcalling', 'false',
            '--AlignProcessing-cpus', '6',
            '--FastqAlign-cpus', '6', 
            '--FastqAlign-create-report', 'True', 
            '--GetFastq-fastq1-url', '',
            '--GetFastq-fastq2-url', '',
            '--GetFastq-from-ebi', 'False',
            '--GetFastq-paired-end', 'True',
            '--ReferenceGenome-ref-url', 'ftp://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit',
            '--ReferenceGenome-from2bit', 'True',
            '--GlobalParams-base-dir', path.abspath(path.curdir),
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'hg19'])
