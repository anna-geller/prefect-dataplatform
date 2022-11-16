import awswrangler as wr
from datetime import datetime
from typing import List
import pandas as pd
from prefect import task, get_run_logger
from dataplatform.blocks.postgres_pandas import PostgresPandas


@task(name="🗂️files to process")
def get_files_to_process(year: int = 2022, service_type: str = "yellow") -> List[str]:
    files = wr.s3.list_objects(f"s3://nyc-tlc/trip data/{service_type}_tripdata_{year}")
    return [f.replace("s3://nyc-tlc/trip data/", "") for f in files]


@task
def extract_from_s3(file_name: str) -> pd.DataFrame:
    logger = get_run_logger()
    raw_df = wr.s3.read_parquet(f"s3://nyc-tlc/trip data/{file_name}")
    logger.info("Extracted %s with %d rows", file_name, len(raw_df))
    return raw_df


@task
def transform(df: pd.DataFrame, file_name: str, service_type: str = "yellow") -> pd.DataFrame:
    df["file"] = file_name
    df[service_type] = service_type
    df["ingested"] = datetime.utcnow().isoformat()
    return df


@task
def load_to_postgres(df: pd.DataFrame, tbl: str, if_exists: str = "append") -> None:
    logger = get_run_logger()
    block = PostgresPandas.load("default")
    block.load_data(df, tbl, if_exists)
    logger.info("%d rows loaded to table %s", len(df), tbl)


@task
def extract_jaffle_shop(dataset: str) -> pd.DataFrame:
    file = f"https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/{dataset}.csv"
    return pd.read_csv(file)
