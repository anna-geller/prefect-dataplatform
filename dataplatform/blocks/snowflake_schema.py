import contextlib

import pandas as pd
from prefect.blocks.core import Block
from prefect_snowflake.database import SnowflakeConnector
from sqlalchemy import create_engine


class SnowflakeSchema(Block):

    """
    Interact with a Snowflake schema using Pandas.
    Requires pandas and snowflake-sqlalchemy packages to be installed.
    
    Args:
        snowflake_connector (SnowflakeConnector): Schema and credentials for a Snowflake schema.
    
    Example:
        Load stored block:
        ```python
        from dataplatform.blocks import SnowflakeSchema
        schema_block = SnowflakeSchema.load("BLOCK_NAME")
        ```
    """  # noqa

    _block_type_name = "Snowflake Schema"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/2DxzAeTM9eHLDcRQx1FR34/f858a501cdff918d398b39365ec2150f/snowflake.png?h=250"  # noqa
    _block_schema_capabilities = ["load_raw_data", "read_sql"]
    snowflake_connector: SnowflakeConnector

    def _get_connection_string(self) -> str:
        acc_id = self.snowflake_connector.credentials.account
        usr = self.snowflake_connector.credentials.user
        role = self.snowflake_connector.credentials.role or "SYSADMIN"
        pwd = self.snowflake_connector.credentials.password.get_secret_value()
        db = self.snowflake_connector.database
        schema = self.snowflake_connector.schema_
        warehouse = self.snowflake_connector.warehouse
        return f"snowflake://{usr}:{pwd}@{acc_id}/{db}/{schema}?warehouse={warehouse}&role={role}"

    def _get_conn_engine(self):
        conn_string = self._get_connection_string()
        return create_engine(conn_string)

    @contextlib.contextmanager
    def _get_connection(self):
        db_engine = self._get_conn_engine()
        conn = db_engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def read_sql(self, table_or_query: str) -> pd.DataFrame:
        db = self._get_connection_string()
        engine = create_engine(db)
        return pd.read_sql(table_or_query, engine)

    def load_raw_data(self, dataframe: pd.DataFrame, table_name: str) -> None:
        db_engine = self._get_conn_engine()
        dataframe.to_sql(
            table_name,
            schema=self.snowflake_connector.schema_,
            con=db_engine,
            if_exists="replace",
            index=False,
        )
