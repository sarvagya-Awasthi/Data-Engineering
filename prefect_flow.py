from prefect import flow, task
from fetchapi_data import fetch_world_bank_data, transform_data
import pandas as pd


@task(retries=2, retry_delay_seconds=5)
def fetch_task():
    return fetch_world_bank_data()


@task
def transform_task(raw_data):
    return transform_data(raw_data)


@task
def save_task(df):
    df.to_csv("prefect_output.csv", index=False)
    return f"Saved {len(df)} records"


@flow(name="world-bank-prefect-flow", log_prints=True)
def etl_flow():
    raw = fetch_task()
    cleaned = transform_task(raw)
    result = save_task(cleaned)
    return result


if __name__ == "__main__":
    etl_flow()