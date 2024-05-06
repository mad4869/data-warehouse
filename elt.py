import luigi
import sentry_sdk

from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.transform import DbtDebug, DbtDeps, DbtRun, DbtSnapshot, DbtTest
from pipeline.constants.sentry_dsn import SENTRY_DSN

sentry_sdk.init(
    dsn=SENTRY_DSN,
    enable_tracing=True,
)

if __name__ == "__main__":
    luigi.build(
        [Extract(), Load(), DbtDebug(), DbtDeps(), DbtRun(), DbtSnapshot(), DbtTest()],
        local_scheduler=True,
    )