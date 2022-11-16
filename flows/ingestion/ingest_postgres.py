import pandas as pd
from prefect import task, flow, get_run_logger

from dataplatform.blocks import PostgresPandas


@task
def extract(dataset: str) -> pd.DataFrame:
    file = f"https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/{dataset}.csv"
    return pd.read_csv(file)


@flow
def extract_and_load() -> None:
    logger = get_run_logger()
    block = PostgresPandas.load_to_postgres("default")
    datasets = ["raw_customers", "raw_orders", "raw_payments"]
    for dataset in datasets:
        df = extract(dataset)
        block.load_raw_data(df, dataset)
        logger.info("Dataset %s loaded", dataset)


if __name__ == "__main__":
    extract_and_load()
