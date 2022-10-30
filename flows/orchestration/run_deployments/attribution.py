from datetime import date
from prefect import flow
from prefect.deployments import run_deployment


@flow
def attribution_parent(
    start_date: date = date(2020, 2, 1),  # parametrized for backfills
    end_date: date = date.today(),
    dataset_size: int = 10_000,
    ingest: str = "raw-data-marketing",
    transform: str = "dbt-attribution",
    deployment_name: str = "default",
):
    params = dict(start_date=start_date, end_date=end_date, dataset_size=dataset_size)
    run_deployment(
        name=f"{ingest}/{deployment_name}", flow_run_name=ingest, parameters=params
    )
    run_deployment(name=f"{transform}/{deployment_name}", flow_run_name=transform)


if __name__ == "__main__":
    attribution_parent()
