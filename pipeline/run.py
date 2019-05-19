import luigi
from os import path

# Utility clases
from tasks.utils import MetaOutputHandler
from tasks.utils import GlobalParams
# Pipeline task module classes
from tasks.vcf import VariantCalling

class Pipeline(luigi.WrapperTask):
    def requires(self):
        return VariantCalling()

if __name__ == '__main__':
    luigi.run(['Pipeline', 
            '--workers', '3', 
            '--VariantCalling-use_platypus', 'true',
            '--VariantCalling-use_freebayes', 'true',
            '--VariantCalling-use_samtools', 'false',
            '--VariantCalling-use_gatk', 'false',
            '--VariantCalling-use_deepcalling', 'false',
            '--AlignProcessing-cpus', '6',
            '--FastqAlign-cpus', '6', 
            '--FastqAlign-create-report', 'True', 
            '--GetFastq-fastq1-url', '',
            '--GetFastq-fastq1-url', '',
            '--GetFastq-fastq1-from-ebi', 'False',
            '--GetFastq-fastq1-paired-end', 'True',
            '--ReferenceGenome-ref-url', 'ftp://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit',
            '--ReferenceGenome-from2bit', 'True',
            '--GlobalParams-base-dir', './experiment',
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'hg19'])
