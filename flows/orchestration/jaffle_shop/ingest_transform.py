from datetime import date
from prefect import flow

from flows.transformation.jaffle_shop.dbt_run_from_manifest import dbt_jaffle_shop
from flows.ingestion.ingest_jaffle_shop import raw_data_jaffle_shop
from flows.analytics.dashboards import dashboards
from flows.ml.sales_forecast import sales_forecast


@flow
def jaffle_shop_ingest_transform(
    start_date: date = date(2022, 11, 1),  # parametrized for backfills
    end_date: date = date.today(),
    dataset_size: int = 10_000,
):
    raw_data_jaffle_shop(start_date, end_date, dataset_size)
    dbt_jaffle_shop()
    dashboards()
    sales_forecast()


if __name__ == "__main__":
    jaffle_shop_ingest_transform()
