from prefect import flow
from dataplatform.blocks.dbt import Dbt


@flow
def dbt_jaffle_shop():
    dbt = Dbt.load("jaffle-shop")
    dbt.dbt_cli("dbt compile")
    dbt.dbt_run_from_manifest()


if __name__ == "__main__":
    dbt_jaffle_shop()
