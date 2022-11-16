from prefect_gcp.credentials import GcpCredentials

from dataplatform.blocks import PostgresPandas, BigQueryPandas

postgres_block = PostgresPandas(user_name="postgres", password="postgres")
postgres_block.save("default", overwrite=True)

block = BigQueryPandas(
    credentials=GcpCredentials.load("default"),
    project="prefect-community",
)
block.save("default", overwrite=True)
