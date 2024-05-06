import luigi
import sentry_sdk

from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.transform import DbtDebug, DbtDeps, DbtRun, DbtSnapshot, DbtTest

sentry_sdk.init(
    dsn="https://245f925fd0fe886dbe02afd9033caaa6@o414765.ingest.us.sentry.io/4507199769804800",
    enable_tracing=True,
)

if __name__ == "__main__":
    luigi.build(
        [Extract(), Load(), DbtDebug(), DbtDeps(), DbtRun(), DbtSnapshot(), DbtTest()],
        local_scheduler=True,
    )
