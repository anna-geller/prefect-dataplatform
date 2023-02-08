from dotenv import load_dotenv
import json
import os
from prefect.blocks.notifications import SlackWebhook
from prefect.filesystems import S3, GCS, Azure, GitHub
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector

from dataplatform.blocks.dbt import Dbt
from dataplatform.blocks.snowflake_pandas import SnowflakePandas
from dataplatform.blocks.workspace import Workspace
from dataplatform.deploy_utils import save_block, DEFAULT_BLOCK
from dataplatform.environment import get_env

load_dotenv()


slack = SlackWebhook(url=os.environ.get("SLACK_WEBHOOK_URL", DEFAULT_BLOCK))
save_block(slack)


workspace = Workspace(
    name=get_env(),
    block_name=DEFAULT_BLOCK,
    metadata=dict(workspace_owner="Data Engineering", environment="Development"),
)
save_block(workspace)

snowflake_creds = SnowflakeCredentials(
    user=os.environ.get("SNOWFLAKE_USER", DEFAULT_BLOCK),
    password=os.environ.get("SNOWFLAKE_PASSWORD", DEFAULT_BLOCK),
    account=os.environ.get("SNOWFLAKE_ACCOUNT", DEFAULT_BLOCK),
)
save_block(snowflake_creds)


snowflake_connector = SnowflakeConnector(
    schema=os.environ.get("SNOWFLAKE_SCHEMA", DEFAULT_BLOCK),
    database=os.environ.get("SNOWFLAKE_DATABASE", DEFAULT_BLOCK),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", DEFAULT_BLOCK),
    credentials=SnowflakeCredentials.load(DEFAULT_BLOCK),
)
save_block(snowflake_connector)


dbt_cli_profile = DbtCliProfile(
    name="dbt_dwh_models",
    target=DEFAULT_BLOCK,
    target_configs=SnowflakeTargetConfigs(
        connector=SnowflakeConnector.load(DEFAULT_BLOCK)
    ),
)
save_block(dbt_cli_profile)

pd = SnowflakePandas(snowflake_connector=SnowflakeConnector.load(DEFAULT_BLOCK))
save_block(pd)

dbt_cloud = DbtCloudCredentials(
    account_id=os.environ.get("DBT_CLOUD_ACCOUNT_ID", 12345),
    api_key=os.environ.get("DBT_CLOUD_API_KEY", DEFAULT_BLOCK),
)
save_block(dbt_cloud)

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

gh = GitHub(
    repository="https://github.com/anna-geller/prefect-dataplatform.git",
    reference=os.environ.get("GITHUB_DATAPLATFORM_BRANCH", "main"),
    # access_token is needed for private repositories, supported in Prefect>=2.6.2
    # access_token=os.environ.get("GITHUB_PERSONAL_ACCESS_TOKEN", DEFAULT_BLOCK),
)
save_block(gh)

s3 = S3(
    bucket_path=os.environ.get("AWS_S3_BUCKET_NAME", DEFAULT_BLOCK),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", DEFAULT_BLOCK),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", DEFAULT_BLOCK),
)
save_block(s3)

az = Azure(
    bucket_path=os.environ.get("AZURE_BUCKET_PATH", DEFAULT_BLOCK),
    azure_storage_connection_string=os.environ.get(
        "AZURE_STORAGE_CONNECTION_STRING", DEFAULT_BLOCK
    ),
)
save_block(az)

gcs = GCS(
    bucket_path=os.environ.get("GCS_BUCKET_PATH", DEFAULT_BLOCK),
    service_account_info=str(
        json.load(
            open(os.environ.get("GCS_SERVICE_ACCOUNT_INFO", "utilities/dummy_sa.json"))
        )
    ),
)
save_block(gcs)


gh_dbt_jaffle_shop = GitHub(
    repository="https://github.com/anna-geller/dbt-jaffle-shop.git",
    reference="main",
    access_token=os.environ.get("GITHUB_PERSONAL_ACCESS_TOKEN", "dummy"),
)
gh_dbt_jaffle_shop.save("dbt-jaffle-shop", overwrite=True)

gh_dbt_attribution = GitHub(
    repository="https://github.com/anna-geller/dbt-attribution.git",
    reference="main",
    access_token=os.environ.get("GITHUB_PERSONAL_ACCESS_TOKEN", "dummy"),
)
gh_dbt_attribution.save("dbt-attribution", overwrite=True)
