"""
docker run --restart always --name postgres14 --net dev -v postgres_data:/var/lib/postgresql/data -p 5432:5432 -d -e POSTGRES_PASSWORD=postgres postgres:14
"""
import pandas as pd
from prefect.blocks.core import Block
from pydantic import SecretStr
from sqlalchemy import create_engine
from typing import Optional


class PostgresPandas(Block):

    """
    Interact with a PostgresPandas database using Pandas.
    Requires pandas and sqlalchemy packages to be installed.

    Args:
        user_name: name of the database user
        password: password of the database user
        db_name: database name
        db_hostname: host of the database server
        db_port: database port

    Example:
        Load stored block:
        ```python
        from dataplatform.blocks import PostgresPandas
        block = PostgresPandas.load("BLOCK_NAME")
        ```
    """  # noqa

    _block_type_name = "PostgresPandas"
    _logo_url = "https://upload.wikimedia.org/wikipedia/commons/2/2e/Pg_logo.png?h=250"
    _block_schema_capabilities = ["load_data", "read_sql"]
    user_name: SecretStr
    password: SecretStr
    db_name: Optional[str] = "postgres"
    db_hostname: Optional[str] = "localhost"
    db_schema: Optional[str] = "public"
    db_port: Optional[int] = 5432

    def _get_connection_string(self):
        usr = self.user_name.get_secret_value()
        pwd = self.password.get_secret_value()
        return (
            f"postgresql://{usr}:{pwd}@{self.db_hostname}:{self.db_port}/{self.db_name}"
        )

    def read_sql(self, table_or_query: str) -> pd.DataFrame:
        db = self._get_connection_string()
        engine = create_engine(db)
        return pd.read_sql(table_or_query, engine)

    def load_data(
        self, dataframe: pd.DataFrame, table_name: str, if_exists="replace"
    ) -> None:
        conn_string = self._get_connection_string()
        db_engine = create_engine(conn_string)
        dataframe.to_sql(
            table_name,
            schema=self.db_schema,
            con=db_engine,
            if_exists=if_exists,
            index=False,
        )


if __name__ == "__main__":
    postgres_block = PostgresPandas(user_name="postgres", password="postgres")
    postgres_block.save("default", overwrite=True)
