from prefect import flow, task
from prefect.logging import get_run_logger

from etl.main import mainFull
@flow
def main():
    mainFull()

if __name__ == "__main__":
    main()
