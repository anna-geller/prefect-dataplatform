from prefect import flow, Task
from prefect_fivetran import FivetranCredentials
from prefect_fivetran.connectors import fivetran_sync_flow


@flow
def my_flow():
    fivetran_result = await fivetran_sync_flow(
        fivetran_credentials=FivetranCredentials.load("default"),
        connector_id="my_connector_id",
        schedule_type="my_schedule_type",
        poll_status_every_n_seconds=30,
    )


my_flow()
