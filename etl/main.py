from prefect import flow, task
from prefect.logging import get_run_logger
import subprocess
import os
from etl.import2raw.import2raw import import2raw

@task
def run_dbt_seed():
    logger = get_run_logger()
    logger.info("Running dbt seed...")
    dbt_project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dbt/sch_tranform'))
    os.environ["DBT_PROFILES_DIR"] = dbt_project_dir
    result = subprocess.run([
        "dbt", "seed", "--project-dir", dbt_project_dir
    ], capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("dbt seed failed")

@task
def run_dbt_run():
    logger = get_run_logger()
    logger.info("Running dbt run...")
    dbt_project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dbt/sch_tranform'))
    os.environ["DBT_PROFILES_DIR"] = dbt_project_dir
    result = subprocess.run([
        "dbt", "run", "--project-dir", dbt_project_dir
    ], capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("dbt run failed")


@flow
def mainFull():
    import2raw()
    run_dbt_seed()
    run_dbt_run()



