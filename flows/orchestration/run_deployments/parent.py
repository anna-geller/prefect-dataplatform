from datetime import date
from prefect import flow
from prefect.deployments import run_deployment


@flow
def parent(
    start_date: date = date(2022, 11, 1),  # parametrized for backfills
    end_date: date = date.today(),
    deployment_name: str = "local-process",
):
    params = dict(start_date=start_date, end_date=end_date)
    run_deployment(
        name=f"raw-data-jaffle-shop/{deployment_name}",
        flow_run_name="raw-data-shop",
        parameters=params,
    )
    run_deployment(
        name=f"raw-data-marketing/{deployment_name}",
        flow_run_name="raw-data-marketing",
        parameters=params,
    )
    run_deployment(name=f"dbt-jaffle-shop/{deployment_name}", flow_run_name="shop")
    run_deployment(
        name=f"dbt-attribution/{deployment_name}", flow_run_name="attribution"
    )
    run_deployment(name=f"dashboards/{deployment_name}", flow_run_name="dashboards")
    run_deployment(name=f"sales-forecast/{deployment_name}", flow_run_name="forecast")


if __name__ == "__main__":
    parent()
