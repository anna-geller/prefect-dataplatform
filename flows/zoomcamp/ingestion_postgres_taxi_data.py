"""
select file, count(*) as nr_rows from yellow_tripdata group by file order by file;
"""
from prefect import flow
from prefect.task_runners import SequentialTaskRunner

from dataplatform.tasks import (
    get_files_to_process,
    extract,
    transform,
    load_to_postgres,
)


@flow  # running without task runner, tasks executed in a flow run process sequentially
def ingestion_postgres_taxi_data(
    table: str = "yellow_tripdata",
    file: str = "yellow_tripdata_2022-01.parquet",
    if_exists: str = "append",
) -> None:
    df = extract.with_options(name=f"extract_{file}")(file)
    df = transform.with_options(name=f"transform_{file}")(df, file)
    load_to_postgres.with_options(name=f"🚀load_{table}")(df, table, if_exists)


@flow(task_runner=SequentialTaskRunner())
def parent_ingestion_postgres_taxi_data(
    table: str = "yellow_tripdata",
    year: int = 2022,
    service_type: str = "yellow",
    if_exists: str = "append",
) -> None:
    files = get_files_to_process(year, service_type)
    for file in files:
        df = extract.with_options(name=f"extract_{file}").submit(file)
        df = transform.with_options(name=f"transform_{file}").submit(
            df, file, service_type
        )
        load_to_postgres.with_options(name=f"load_{file}").submit(
            df.result().head(100), table, if_exists
        )


if __name__ == "__main__":
    parent_ingestion_postgres_taxi_data()
