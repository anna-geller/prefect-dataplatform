from datetime import date
from prefect import flow

from flows.ingestion.marketing_config import Frequency
from flows.ingestion.ingest_marketing_data import raw_data_marketing
from flows.transformation.attribution.dbt_build import dbt_attribution


@flow
def attribution_ingest_transform(
    start_date: date = date(2022, 11, 1),  # parametrized for backfills
    end_date: date = date.today(),
    interval: Frequency = "D",
):
    raw_data_marketing(start_date, end_date, interval)
    dbt_attribution()


if __name__ == "__main__":
    attribution_ingest_transform()
