import subprocess

if __name__ == "__main__":
    blocks = [
        "dbt-cli-profile/default",
        "dbt/attribution",
        "dbt/jaffle-shop",
        "dbt-cloud-credentials/default",
        "snowflake-connector/default",
        "snowflake-credentials/default",
        "snowflake-pandas/default",
        "workspace/default",
        "azure/default",
        "s3/default",
        "gcs/default",
        "docker-container/default",
        "kubernetes-job/default",
        "process/default",
        "slack-webhook/default",
        "github/default",
        "github/dbt-jaffle-shop",
        "github/dbt-attribution",
    ]
    for block in blocks:
        subprocess.run(f"prefect block delete {block}", shell=True)
    block_types = [
        "workspace",
        "snowflake-schema",
        "dbt",
    ]
    for block_type in block_types:
        subprocess.run(f"prefect block type delete {block_type}", shell=True)
