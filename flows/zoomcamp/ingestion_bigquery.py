import pandas as pd
from prefect import task, flow, get_run_logger
from dataplatform.blocks import BigQueryPandas
from typing import List


@task
def extract(dataset: str) -> pd.DataFrame:
    file = f"https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/{dataset}.csv"
    return pd.read_csv(file)


@task
def load(df: pd.DataFrame, tbl: str, **kwargs) -> None:
    logger = get_run_logger()
    block = BigQueryPandas.load("default")
    block.load_data(dataframe=df, table_name=tbl, **kwargs)
    ref = block.credentials.get_bigquery_client().get_table(tbl)
    logger.info(
        "Df loaded ✅ table %s has now %d rows and %s MB",
        tbl,
        ref.num_rows,
        ref.num_bytes / 1_000_000,
    )


@flow
def ingestion_bigquery(
    dataset: str = "jaffle_shop2",
    tables: List[str] = ["raw_customers", "raw_orders", "raw_payments"],
    **kwargs,
) -> None:
    block = BigQueryPandas.load("default")
    block.create_dataset_if_not_exists(dataset)
    for table in tables:
        bq_table = f"{dataset}.{table}"
        df = extract.with_options(name=f"🗂️ extract_{table}")(table)
        load.with_options(name=f"🚀 load_{table}")(df, bq_table, **kwargs)


if __name__ == "__main__":
    ingestion_bigquery()
