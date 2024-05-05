import luigi

from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.transform import DbtDebug, DbtDeps, DbtRun, DbtSnapshot, DbtTest

if __name__ == "__main__":
    luigi.build([
        Extract(),
        Load(),
        DbtDebug(),
        DbtDeps(),
        DbtRun(),
        DbtSnapshot(),
        DbtTest()
        ],
        local_scheduler=True
    )