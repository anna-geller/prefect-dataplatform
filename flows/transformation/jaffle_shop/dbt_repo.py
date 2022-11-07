from prefect import flow
from prefect.filesystems import GitHub
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.credentials import DbtCliProfile


def dbt(command: str, path: str) -> None:
    trigger_dbt_cli_command.with_options(name=command)(
        command,
        project_dir=path,
        overwrite_profiles=True,
        dbt_cli_profile=DbtCliProfile.load("default"),
    )


@flow(retries=3, retry_delay_seconds=30)
def dbt_jaffle_shop(
    dbt_command: str = "dbt build",
    repository: str = "dbt-jaffle-shop",
):
    gh = GitHub.load(repository)
    gh.get_directory(local_path=repository)
    dbt(dbt_command, repository)


if __name__ == "__main__":
    dbt_jaffle_shop()
