from dotenv import load_dotenv
from dataplatform.blocks.dbt import Dbt
from dataplatform.blocks.workspace import Workspace
from dataplatform.blocks.snowflake_pandas import SnowflakePandas
from dataplatform.environment import get_env
from dataplatform.deploy_utils import save_block
import os
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect.filesystems import S3, GCS, Azure
from prefect.blocks.notifications import SlackWebhook


load_dotenv()

DEFAULT_BLOCK = "default"

slack = SlackWebhook(url=os.environ.get("SLACK_WEBHOOK_URL", "dummy"))
save_block(slack, DEFAULT_BLOCK)


workspace = Workspace(
    name=get_env(),
    block_name=DEFAULT_BLOCK,
    settings=dict(workspace_owner="Data Engineering", environment="Development"),
)
save_block(workspace, DEFAULT_BLOCK)

snowflake_creds = SnowflakeCredentials(
    user=os.environ.get("SNOWFLAKE_USER", "dummy"),
    password=os.environ.get("SNOWFLAKE_PASSWORD", "dummy"),
    account=os.environ.get("SNOWFLAKE_ACCOUNT", "dummy"),
)
save_block(snowflake_creds, DEFAULT_BLOCK)


snowflake_connector = SnowflakeConnector(
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "dummy"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "dummy"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "dummy"),
    credentials=SnowflakeCredentials.load(DEFAULT_BLOCK),
)
save_block(snowflake_connector, DEFAULT_BLOCK)


dbt_cli_profile = DbtCliProfile(
    name="dbt_dwh_models",
    target=DEFAULT_BLOCK,
    target_configs=SnowflakeTargetConfigs(
        connector=SnowflakeConnector.load(DEFAULT_BLOCK)
    ),
)
save_block(dbt_cli_profile)

pd = SnowflakePandas(snowflake_connector=SnowflakeConnector.load(DEFAULT_BLOCK))
save_block(pd, DEFAULT_BLOCK)

dbt_jaffle_shop = Dbt(
    workspace=Workspace.load(DEFAULT_BLOCK),
    # because it's 3 directories up from dbt flows to the root directory in which dbt project resides
    path_to_dbt_project="dbt_jaffle_shop",
)
save_block(dbt_jaffle_shop, "jaffle-shop")

dbt_attribution = Dbt(
    workspace=Workspace.load(DEFAULT_BLOCK),
    path_to_dbt_project="dbt_attribution",
    default_dbt_cli_emoji="🤖 ",
    default_dbt_model_emoji="💰 ",
    default_dbt_test_emoji="✅ ",
)
save_block(dbt_attribution, "attribution")

s3 = S3(
    bucket_path=os.environ.get("AWS_S3_BUCKET_NAME", "dummy"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "dummy"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "dummy"),
)
save_block(s3, DEFAULT_BLOCK)

az = Azure(
    bucket_path=os.environ.get("AZURE_BUCKET_PATH", "dummy"),
    azure_storage_connection_string=os.environ.get(
        "AZURE_STORAGE_CONNECTION_STRING", "dummy"
    ),
)
save_block(az, DEFAULT_BLOCK)

gcs = GCS(
    bucket_path=os.environ.get("GCS_BUCKET_PATH", "dummy"),
    service_account_info=os.environ.get("GCS_SERVICE_ACCOUNT_INFO", "dummy"),
)
save_block(gcs, DEFAULT_BLOCK)
