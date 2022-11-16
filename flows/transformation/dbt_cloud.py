from prefect import flow
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion


@flow
def run_dbt_job_flow():
    trigger_dbt_cloud_job_run_and_wait_for_completion(
        dbt_cloud_credentials=DbtCloudCredentials.load("default"), job_id=154217
    )


if __name__ == "__main__":
    run_dbt_job_flow()
