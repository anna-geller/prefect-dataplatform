from prefect import task, flow, get_run_logger


@task
def update_customers_dashboards():
    logger = get_run_logger()
    # your logic here
    logger.info("Customers dashboard extracts updated! 📊")


@task
def update_sales_dashboards():
    logger = get_run_logger()
    # your logic here
    logger.info("Sales dashboard extracts updated! 📊")


@flow
def dashboards():
    update_customers_dashboards()
    update_sales_dashboards()


if __name__ == "__main__":
    dashboards()
