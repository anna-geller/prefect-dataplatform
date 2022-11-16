from prefect import flow
from typing import List

from dataplatform.tasks import load_to_postgres, extract_jaffle_shop


@flow
def ingestion_postgres(
    tables: List[str] = ["raw_customers", "raw_orders", "raw_payments"],
    if_exists: str = "replace",
) -> None:
    for table in tables:
        df = extract_jaffle_shop.with_options(name=f"🗂️extract_{table}").submit(table)
        load_to_postgres.with_options(name=f"🚀load_{table}").submit(df, table, if_exists)


if __name__ == "__main__":
    ingestion_postgres()
