from pathlib import Path
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.credentials import DbtCliProfile


def dbt(command: str = "dbt debug") -> None:
    trigger_dbt_cli_command.with_options(name=command)(
        command,
        project_dir=Path(__file__)
        .parent.parent.parent.parent.joinpath("dbt_jaffle_shop")
        .expanduser(),
        overwrite_profiles=True,
        dbt_cli_profile=DbtCliProfile.load("default"),
    )


@flow
def dbt_jaffle_shop(dbt_command: str = "dbt build"):
    dbt(dbt_command)


if __name__ == "__main__":
    dbt_jaffle_shop()
