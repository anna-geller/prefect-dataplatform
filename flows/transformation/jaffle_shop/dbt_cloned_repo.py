from pathlib import Path
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.credentials import DbtCliProfile
from dataplatform.repository import pull_repo


def dbt(command: str, path: str) -> None:
    trigger_dbt_cli_command.with_options(name=command)(
        command,
        project_dir=path,
        overwrite_profiles=True,
        dbt_cli_profile=DbtCliProfile.load("default"),
    )


@flow(retries=3, retry_delay_seconds=30)
def dbt_cloned_repo(
    dbt_command: str = "dbt build",
    repository: str = "jaffle_shop",
    github_organization: str = "anna-geller",
    path: str = Path(__file__)
    .parent.parent.parent.parent.joinpath("jaffle_shop")
    .expanduser(),
):
    pull_repo(repo=repository, org=github_organization, path=path)
    dbt(dbt_command, path)


if __name__ == "__main__":
    dbt_cloned_repo()
