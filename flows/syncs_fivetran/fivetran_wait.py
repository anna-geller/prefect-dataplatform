"""
pip install prefect_fivetran
prefect block register -m prefect_fivetran
"""
from prefect import flow
from prefect_fivetran import FivetranCredentials
from prefect_fivetran.connectors import (
    wait_for_fivetran_connector_sync,
    start_fivetran_connector_sync,
)


@flow
def example_flow(connector_id: str):
    fivetran_credentials = FivetranCredentials.load("default")

    last_sync = start_fivetran_connector_sync(
        connector_id=connector_id,
        fivetran_credentials=fivetran_credentials,
    )

    return wait_for_fivetran_connector_sync(
        connector_id=connector_id,
        fivetran_credentials=fivetran_credentials,
        previous_completed_at=last_sync,
        poll_status_every_n_seconds=60,
    )


if __name__ == "__main__":
    example_flow("bereft_indices")
