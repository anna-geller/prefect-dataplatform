import pandas as pd
from prefect import task, flow, get_run_logger
from dataplatform.blocks import PostgresPandas
from typing import List


@task
def extract(dataset: str) -> pd.DataFrame:
    file = f"https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/{dataset}.csv"
    return pd.read_csv(file)


@task
def load(df: pd.DataFrame, dataset: str) -> None:
    logger = get_run_logger()
    block = PostgresPandas.load("default")
    block.load_data(df, dataset)
    logger.info("%d rows loaded to table %s", len(df), dataset)


@flow
def ingestion_postgres(
        tables: List[str] = ["raw_customers", "raw_orders", "raw_payments"]
) -> None:
    for table in tables:
        df = extract.with_options(name=f"🗂️ extract_{table}")(table)
        load.with_options(name=f"🚀 load_{table}")(df, table)


if __name__ == "__main__":
    ingestion_postgres()
