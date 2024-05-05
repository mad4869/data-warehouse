import subprocess as sp
import luigi
import datetime
import time
import logging

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default=datetime.datetime.now())

class DbtTask(luigi.Task):
    command = luigi.Parameter()

    current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(f"/home/mad4869/Documents/pacmann/data-storage/data-warehouse/logs/dbt_{self.command}/dbt_{self.command}_logs_{self.current_timestamp}.log")
    
    def run(self):
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run(
                    f"cd ./dwh_dbt/ && dbt {self.command}",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True
                )

                if p1.returncode == 0:
                    logging.info(f"Success running dbt {self.command} process")
                else:
                    logging.error(f"Failed running dbt {self.command} process")

            time.sleep(2)
        except Exception as e:
            logging.error(f"Failed process: {e}")

class DbtDebug(DbtTask):
    command = "debug"

class DbtDeps(DbtTask):
    command = "deps"

    def requires(self):
        return DbtDebug()

class DbtRun(DbtTask):
    command = "run"

    def requires(self):
        return DbtDeps()

class DbtSnapshot(DbtTask):
    command = "snapshot"

    def requires(self):
        return DbtRun()

class DbtTest(DbtTask):
    command = "test"

    def requires(self):
        return DbtSnapshot()