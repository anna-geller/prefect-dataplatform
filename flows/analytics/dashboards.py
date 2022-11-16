from prefect import task, flow, get_run_logger
from typing import Any, Dict

from dataplatform.blocks import Workspace, SnowflakePandas


@task
def update_customers_dashboards() -> None:
    logger = get_run_logger()
    # your logic here - might be clearing cash of your BI tool
    logger.info("Customers dashboard extracts updated! 📊")


@task
def update_sales_dashboards() -> None:
    logger = get_run_logger()
    # your logic here - might be clearing cash of your BI tool
    logger.info("Sales dashboard extracts updated! 📊")


@task
def extract_current_kpis() -> Dict[str, Any]:
    sql_revenue = "SELECT SUM(REVENUE) as revenue FROM STG_CUSTOMER_CONVERSIONS;"
    sql_orders = "SELECT COUNT(ORDER_ID) as nr_orders FROM ORDERS;"
    block = SnowflakePandas.load("default")
    revenue = block.read_sql(sql_revenue)["revenue"][0]
    orders = block.read_sql(sql_orders)["nr_orders"][0]
    return dict(revenue=revenue, nr_orders=orders)


@task
def send_kpi_report(kpis: Dict[str, Any]) -> None:
    workspace = Workspace.load("default")
    for key, val in kpis.items():
        workspace.send_alert(f"The current {key} KPI is {val} 🚀")


@task
def reverse_etl(kpis: Dict[str, Any]) -> None:
    logger = get_run_logger()
    logger.info(kpis)  # loads kpis to source systems


@flow
def dashboards():
    update_customers_dashboards.submit()
    update_sales_dashboards.submit()
    kpis = extract_current_kpis.submit()
    send_kpi_report.submit(kpis)
    reverse_etl.submit(kpis)


if __name__ == "__main__":
    dashboards()
