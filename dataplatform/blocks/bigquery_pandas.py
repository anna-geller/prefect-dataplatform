"""
pip install prefect-gcp
pip install pydata-google-auth
pip install google-auth
pip install google-auth-oauthlib
pip install google-cloud-bigquery
pip install google-cloud-bigquery-storage
pip install pandas-gbq -U
pip install google-cloud-storage
- set if_exists to "append" when needed

Example query:
df = block.read_sql("SELECT * FROM project.dataset.table LIMIT 10")
"""
from google.cloud.exceptions import NotFound
import pandas as pd
from prefect.blocks.core import Block
from prefect_gcp.credentials import GcpCredentials
from typing import Optional


class BigQueryPandas(Block):

    """
    Interact with BigQueryPandas
    Requires prefect-gcp and pandas-gbq packages to be installed.

    Args:
        credentials: GcpCredentials block

    Example:
        Load stored block:
        ```python
        from dataplatform.blocks import BigQueryPandas
        block = BigQueryPandas.load("BLOCK_NAME")
        ```
    """  # noqa

    _block_type_name = "BigQueryPandas"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4CD4wwbiIKPkZDt4U3TEuW/c112fe85653da054b6d5334ef662bec4/gcp.png?h=250"  # noqa
    _block_schema_capabilities = ["load_data", "read_sql", "create_dataset_if_not_exists"]
    credentials: GcpCredentials

    def create_dataset_if_not_exists(self, dataset: str) -> None:
        client = self.credentials.get_bigquery_client()
        try:
            client.get_dataset(dataset)
        except NotFound:
            client.create_dataset(dataset)

    def read_sql(self, query: str, **kwargs) -> pd.DataFrame:
        return pd.read_gbq(
            query=query,
            credentials=self.credentials.get_credentials_from_service_account(),
            **kwargs,
        )

    def load_data(
        self,
        dataframe: pd.DataFrame,
        table_name: str,  # dataset.tablename
        chunksize: Optional[int] = 10_000,
        if_exists: str = "append",
        **kwargs,
    ) -> None:
        dataframe.to_gbq(
            destination_table=table_name,
            chunksize=chunksize,
            if_exists=if_exists,
            credentials=self.credentials.get_credentials_from_service_account(),
            **kwargs,
        )


if __name__ == "__main__":
    block = BigQueryPandas(
        credentials=GcpCredentials.load("default"),
        project="prefect-community",
    )
    block.save("default", overwrite=True)
