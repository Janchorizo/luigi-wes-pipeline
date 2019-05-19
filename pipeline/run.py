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
