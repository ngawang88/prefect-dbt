from prefect import task
import pandas as pd

@task
def load_csv_to_df(csv_path):
    df = pd.read_csv(csv_path)
    return df