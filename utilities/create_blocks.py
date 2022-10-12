from dotenv import load_dotenv
from dataplatform.blocks.dbt import Dbt
from dataplatform.blocks.workspace import Workspace
from dataplatform.blocks.snowflake_schema import SnowflakeSchema
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

slack = SlackWebhook(url=os.environ.get("SLACK_WEBHOOK_URL", "demo_dummy_value"))
save_block(slack, DEFAULT_BLOCK)


workspace = Workspace(
    name=get_env(),
    block_name=DEFAULT_BLOCK,
    settings=dict(workspace_owner="Data Engineering", environment="Development"),
)
save_block(workspace, DEFAULT_BLOCK)

snowflake_creds = SnowflakeCredentials(
    user=os.environ.get("SNOWFLAKE_USER", "demo_dummy_value"),
    password=os.environ.get("SNOWFLAKE_PASSWORD", "demo_dummy_value"),
    account=os.environ.get("SNOWFLAKE_ACCOUNT", "demo_dummy_value"),
)
save_block(snowflake_creds, DEFAULT_BLOCK)


snowflake_connector = SnowflakeConnector(
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "demo_dummy_value"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "demo_dummy_value"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "demo_dummy_value"),
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

schema = SnowflakeSchema(snowflake_connector=SnowflakeConnector.load(DEFAULT_BLOCK))
save_block(schema, DEFAULT_BLOCK)

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
    bucket_path=os.environ.get("AWS_S3_BUCKET_NAME", "demo_dummy_value"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "demo_dummy_value"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "demo_dummy_value"),
)
save_block(s3, DEFAULT_BLOCK)

az = Azure(
    bucket_path=os.environ.get("AZURE_BUCKET_PATH", "demo_dummy_value"),
    azure_storage_connection_string=os.environ.get(
        "AZURE_STORAGE_CONNECTION_STRING", "demo_dummy_value"
    ),
)
save_block(az, DEFAULT_BLOCK)

gcs = GCS(
    bucket_path=os.environ.get("GCS_BUCKET_PATH", "demo_dummy_value"),
    service_account_info=os.environ.get("GCS_SERVICE_ACCOUNT_INFO", "demo_dummy_value"),
)
save_block(gcs, DEFAULT_BLOCK)
