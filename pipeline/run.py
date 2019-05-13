import luigi
from os import path

# Utility clases
from tasks.utils import MetaOutputHandler
from tasks.utils import GlobalParams
# Pipeline task module classes
from tasks.reference import ReferenceGenome

class Pipeline(luigi.WrapperTask):
    def requires(self):
        return ReferenceGenome()

if __name__ == '__main__':
    luigi.run(['Pipeline', 
            '--ReferenceGenome-ref-url', 'ftp://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit',
            '--workers', '2',
            '--ReferenceGenome-from2bit', 'True',
            '--GlobalParams-base-dir', path.abspath(path.curdir),
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'get_ref_genome'])
