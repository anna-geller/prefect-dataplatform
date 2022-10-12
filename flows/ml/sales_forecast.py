from prefect import task, flow, get_run_logger


@task
def get_training_set():
    return dict(data=21)


@task
def apply_ml_model(training_set):
    result = training_set["data"] * 2
    logger = get_run_logger()
    logger.info("Final result: %s 🤖", result)
    return result


@flow
def sales_forecast():
    data = get_training_set()
    apply_ml_model(data)


if __name__ == "__main__":
    sales_forecast()
