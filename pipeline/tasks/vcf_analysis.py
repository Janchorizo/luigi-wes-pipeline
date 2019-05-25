import itertools
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from os import path
from tasks.utils import MetaOutputHandler
from tasks.utils import Wget
from tasks.utils import GlobalParams

from tasks.vcf import VariantCalling

class VcftoolsCompare(ExternalProgramTask):
    vcf1 = luigi.Parameter()
    vcf2 = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        filename = lambda f: path.split(f)[-1].split('.')[0]

        return luigi.LocalTarget(
            path.join(GlobalParams().base_dir,
            ''.join([filename(self.vcf1), '_vs_', filename(self.vcf2)])))

    def program_args(self):
        return ['vcftools',
            '--vcf',
            self.vcf1,
            '--diff',
            self.vcf2,
            '--not-chr',
            '--diff-site',
            '--out',
            self.output().path
        ]

class VcftoolsAnalysis(ExternalProgramTask):
    vcf = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(self.vcf.replace('.vcf', '.vcf_info'))

    def program_args(self):
        return ['vcftools',
            '--vcf',
            self.vcf,
            '--out',
            self.output().path
        ]

class VariantCallingAnalysis(luigi.Task):
    def requires(self):
        return VariantCalling()

    def outputs(self):
        output = [vcf.path.replace('.vcf','vcf_info') for vcf in self.input().values()]

        return output

    def run(self):
        #yield((VcftoolsAnalysis(vcf=vcf.path) for vcf in self.input().values()))
        if len(self.input()) > 1:
            yield((VcftoolsCompare(vcf1=vcf1.path, vcf2=vcf2.path) \
                for vcf1,vcf2 in itertools.combinations(self.input().values(),2)))
    

if __name__ == '__main__':
    luigi.run(['VariantCallingAnalysis', 
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
            '--GlobalParams-base-dir', './experiment',
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'hg19'])
