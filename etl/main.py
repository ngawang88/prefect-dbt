from prefect import flow, task
from prefect.logging import get_run_logger

from etl.import2raw.import2raw import import2raw

@task
def mainFull():
    import2raw()


