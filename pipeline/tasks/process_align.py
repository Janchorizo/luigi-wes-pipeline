import luigi
from luigi.contrib.external_program import ExternalProgramTask
from os import path
from .utils import MetaOutputHandler
from .utils import Wget
from .utils import GlobalParams

from .reference import ReferenceGenome
from .align import FastqAlign

class SortSam(ExternalProgramTask):
    def requires(self):
        return FastqAlign()

    def output(self):
        return luigi.LocalTarget(
            path.join(GlobalParams().base_dir,
            GlobalParams().exp_name+'.bam')
            )

    def program_args(self):
        return ['samtools', 'sort', '-@', AlignProcessing().cpus, 
            self.input()['sam'].path, '-o', self.output().path
        ]

class IndexBam(ExternalProgramTask):
    def requires(self):
        return SortSam()

    def output(self):
        return luigi.LocalTarget(self.input().path+'.bai')

    def program_args(self):
        return ['samtools', 'index', '-@', AlignProcessing().cpus, self.input().path]

class PicardMarkDuplicates(ExternalProgramTask):
    def requires(self):
        return {'index' : IndexBam(),
            'bam' : SortSam()
        }

    def output(self):
        return {
            'bam' : luigi.LocalTarget( \
                path.join(GlobalParams().base_dir, GlobalParams().exp_name+'_nodup.bam')),
            'metrics' : luigi.LocalTarget( \
                path.join(GlobalParams().base_dir, GlobalParams().exp_name+'_MD.matrix'))
        }

    def program_args(self):
        return ['picard', 'MarkDuplicates', 
            'I='+self.input()['bam'].path, 
            'O='+self.output()['bam'].path, 
            'METRICS_FILE='+self.output()['metrics'].path,
            'REMOVE_DUPLICATES=true'
        ]

class IndexNoDup(ExternalProgramTask):
    def requires(self):
        return PicardMarkDuplicates()

    def output(self):
        return luigi.LocalTarget(self.input()['bam'].path+'.bai')

    def program_args(self):
        return ['samtools', 'index', '-@', AlignProcessing().cpus, self.input()['bam'].path]

class AlignProcessing(MetaOutputHandler, luigi.WrapperTask):
    cpus = luigi.Parameter()

    def requires(self):
        return {
            'bam' : SortSam(),
            'bai' : IndexBam(),
            'bamNoDup' : PicardMarkDuplicates(),
            'indexNoDup' : IndexNoDup()
            }

if __name__ == '__main__':
    luigi.run(['AlignProcessing', 
            '--AlignProcessing-cpus', '6',
            '--FastqAlign-cpus', '6', 
            '--FastqAlign-create-report', 'True', 
            '--GetFastq-fastq1-url', '',
            '--GetFastq-fastq1-url', '',
            '--GetFastq-fastq1-from-ebi', 'False',
            '--GetFastq-fastq1-paired-end', 'True',
            '--ReferenceGenome-ref-url', 'ftp://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit',
            '--ReferenceGenome-from2bit', 'True',
            '--GlobalParams-base-dir', path.abspath(path.curdir),
            '--GlobalParams-log-dir', path.abspath(path.curdir),
            '--GlobalParams-exp-name', 'hg19'])
