"""
prefect deployment build flows/zoomcamp/ingestion_bigquery_taxi_data.py:taxi_data -n yellow -q default -a
prefect deployment build flows/zoomcamp/ingestion_bigquery_taxi_data.py:taxi_data -n yellow -q default -a --param table_name=green_tripdata
prefect deployment build flows/zoomcamp/ingestion_bigquery_taxi_data.py:parent -n yellow -q default -a
prefect deployment build flows/zoomcamp/ingestion_bigquery_taxi_data.py:parent -n yellow -q default -a --param table_name=green_tripdata
"""
from datetime import datetime
import pandas as pd
from prefect import task, flow, get_run_logger
from prefect.blocks.system import JSON
from prefect.task_runners import SequentialTaskRunner

from dataplatform.blocks import BigQueryPandas
from dataplatform.tasks import get_files_to_process, extract, transform


@task
def load(df: pd.DataFrame, file: str, tbl: str, if_exists: str = "append") -> None:
    logger = get_run_logger()
    block = BigQueryPandas.load("default")
    block.load_data(dataframe=df, table_name=tbl, if_exists=if_exists)
    ref = block.credentials.get_bigquery_client().get_table(tbl)
    logger.info(
        "Loaded %s to %s ✅ table now has %d rows and %s GB",
        file,
        tbl,
        ref.num_rows,
        ref.num_bytes / 1_000_000_000,
    )


@task
def update_pocessed_files(
    df: pd.DataFrame,
    file: str,
    tbl: str,
    service_type: str = "yellow",
    reset_block_value: bool = False,
) -> None:
    try:
        block = JSON.load(service_type)
    except ValueError:
        block = JSON(value={})
    files_processed = {} if reset_block_value else block.value
    now = datetime.utcnow().isoformat()
    files_processed[file] = dict(table=tbl, nrows=len(df), ingested_at=now)
    block = JSON(value=files_processed)
    block.save(service_type, overwrite=True)


@task
def check_if_processed(file: str, service_type: str = "yellow") -> bool:
    try:
        block = JSON.load(service_type)
    except ValueError:
        block = JSON(value={})
    logger = get_run_logger()
    files_processed = block.value
    exists = file in files_processed.keys()
    if exists:
        logger.info("File %s already ingested: %s", file, files_processed[file])
    return exists


@flow(task_runner=SequentialTaskRunner())
def taxi_data(
    file: str = "yellow_tripdata_2022-08.parquet",
    dataset: str = "trips_data_all",
    table_name: str = "yellow_tripdata",
    service_type: str = "yellow_tripdata",
    if_exists: str = "replace",
    reset_block_value: bool = True,
):
    tbl = f"{dataset}.{table_name}"
    block = BigQueryPandas.load("default")
    block.create_dataset_if_not_exists(dataset)
    df = extract.with_options(name=f"extract_{file}").submit(file)
    df = transform.with_options(name=f"transform_{file}").submit(df, file)
    load.with_options(name=f"load_{file}").submit(df, file, tbl, if_exists=if_exists)
    update_pocessed_files.with_options(name=f"update_block_{file}").submit(
        df, file, tbl, service_type, reset_block_value
    )


@flow(task_runner=SequentialTaskRunner())
def parent(
    dataset: str = "trips_data",
    table_name: str = "yellow_tripdata",
    year: int = 2022,
    service_type: str = "yellow",
    if_exists: str = "append",
):
    files = get_files_to_process(year, service_type)
    tbl = f"{dataset}.{table_name}"
    block = BigQueryPandas.load("default")
    block.create_dataset_if_not_exists(dataset)
    for file in files:
        if (
            not check_if_processed.with_options(name=f"check_{file}")
            .submit(file, service_type)
            .result()
        ):
            df = extract.with_options(name=f"extract_{file}").submit(file)
            df = transform.with_options(name=f"transform_{file}").submit(
                df.result(), file
            )
            load.with_options(name=f"load_{file}").submit(
                df.result().head(100),
                file,
                tbl,
                if_exists=if_exists,  # TODO remove .head(100) to load full dataset
            )
            update_pocessed_files.with_options(name=f"update_block_{file}").submit(
                df.result(), file, tbl, service_type
            )


if __name__ == "__main__":
    parent()
