import luigi
from luigi.contrib import ExternalProgramTask
from os import path
from utils import MetaOutputHandler
from utils import Wget
from utils import GlobalParams

class GetProgram(ExternalProgramTask):
    def requires(self):
        program_file = path.join(GlobalParams().base_dir, 'twoBitToFa')
        program_url = 'ftp://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/twoBitToFa'

        return WgetFile(url=program_url, output_file=program_file)

    def output(self):
        program_file = path.join(GlobalParams().base_dir, 'twoBitToFa')
        return luigi.LocalTarget(program_file)

    def program_args(self):
        return ['chmod', '700', self.output().path]

class TwoBitToFa(ExternalProgramTask):
    ref_url = luigi.Parameter()

    def requires(self):
        file = path.join(GlobalParams().base_dir, GlobalParams().exp_name+'.2bit')

        return {
            'program': GetProgram(), 
            'file': WgetFile(url=ref_url, output_file=file),
        }    

    def output(self):
        return luigi.LocalTarget(path.join(GlobalParams().base_dir,GlobalParams().exp_name+'.fa'))

    def program_args(self):
        return [self.input()['program'].path, self.input()['file'].path, self.output().path]

def GetReferenceFa(luigi.WrapperTask):
    def requires(self):
        if self.from2bit == True:
            return TwoBitToFa(ref_url=self.ref_url)
        else:
            return Wget()

class FaidxIndex(ExternalProgramTask):
    def requires(self): 
        return TwoBitToFa(from2bit=self.from2bit ,ref_url=self.ref_url)

    def output(self):
        return luigi.LocalTarget(self.input().path+'.fai')

    def program_args(self):
        return ['samtools', 'faidx', self.input().path]

class BwaIndex(ExternalProgramTask):
    def requires(self): 
        return GetReferenceFa(from2bit=self.from2bit ,ref_url=self.ref_url)

    def output(self):
        outputs = set()

        outputs.add(
            luigi.LocalTarget(
                path.join(GlobalParams().base_dir,GlobalParams().exp_name+".fa.amb")))
        outputs.add(
            luigi.LocalTarget(
                path.join(GlobalParams().base_dir,GlobalParams().exp_name+".fa.ann")))
        outputs.add(
            luigi.LocalTarget(
                path.join(GlobalParams().base_dir,GlobalParams().exp_name+".fa.bwt")))
        outputs.add(
            luigi.LocalTarget(
                path.join(GlobalParams().base_dir,GlobalParams().exp_name+".fa.pac")))
        outputs.add(
            luigi.LocalTarget(
                path.join(GlobalParams().base_dir,GlobalParams().exp_name+".fa.sa")))

        return outputs

    def program_args(self):
        return ['bwa', 'index', self.input().path]

class ReferenceGenome(luigi.WrapperTask, MetaOutputPropagating):
    ref_url = luigi.Parameter(default='')
    from2bit = luigi.Parameter(default='')

    def requires(self):
        return {
            'faidx' : FaidxIndex(from2bit=self.from2bit ,ref_url=self.ref_url), \
            'bwa' : BwaIndex(from2bit=self.from2bit ,ref_url=self.ref_url), \
            'fa' : GetReferenceFa(from2bit=self.from2bit ,ref_url=self.ref_url) \
            }

if __name__ == '__main__':
    luigi.run(['ReferenceGenome', 
            '--ReferenceGenome-ref-url', 'ftp://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit',
            '--ReferenceGenome-from2bit', True,
            '--GlobalParams-base-dir', path.abspath(path.curdir),
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'get_ref_genome'])
