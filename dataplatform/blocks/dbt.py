import json
from prefect.blocks.core import Block
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.credentials import DbtCliProfile
from pydantic import Field
from typing import Any

from dataplatform.blocks.workspace import Workspace


class Dbt(Block):

    """
    A block for interacting with dbt

    Args:
        workspace: a Workspace block including key-value pairs about your dbt project
        path_to_dbt_project: relative or absolute path to your project as a string
        retries: max number of retries for any dbt task or flow
        retry_delay_seconds: delay between retries in seconds
        metadata: key-value pairs about your dbt project
        default_dbt_cli_emoji: used to differentiate dbt CLI commands from other tasks
        default_dbt_model_emoji: used to differentiate `dbt run model` from other tasks
        default_dbt_test_emoji: used to differentiate `dbt test model` from other tasks

    Example:
        Load a stored JSON value:
        ```python
        from dataplatform.blocks import Dbt

        dbt = Dbt.load("BLOCK_NAME")
        ```
    """

    _block_type_name = "Dbt"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/5zE9lxfzBHjw3tnEup4wWL/9a001902ed43a84c6c96d23b24622e19/dbt-bit_tm.png?h=250"  # noqa
    _block_schema_capabilities = ["dbt_cli", "dbt_run_from_manifest"]

    workspace: Workspace
    path_to_dbt_project: str
    retries: int = 1
    retry_delay_seconds: int = 10
    metadata: Any = Field(
        default=dict(dbt_project_owner="Analytics Engineering Team"),
    )
    default_dbt_cli_emoji: str = Field(
        default="📦 ",
        description="If you don't want to use emojis, you can set it to an empty string",
    )
    default_dbt_model_emoji: str = Field(
        default="🐟 ",
        description="If you don't want to use emojis, you can set it to an empty string",
    )
    default_dbt_test_emoji: str = Field(
        default="✅ ",
        description="If you don't want to use emojis, you can set it to an empty string",
    )

    @staticmethod
    def get_dbt_profile():
        return DbtCliProfile.load("default")

    def dbt_cli(self, dbt_command: str) -> None:
        state = trigger_dbt_cli_command.with_options(
            name=f"{self.default_dbt_cli_emoji}{dbt_command}",
            retries=self.retries,
            retry_delay_seconds=self.retry_delay_seconds,
        )(
            dbt_command,
            project_dir=self.path_to_dbt_project,
            overwrite_profiles=True,
            dbt_cli_profile=self.get_dbt_profile(),
            return_state=True,
        )
        self.workspace.send_alert_on_failure(state, dbt_command)

    def _dbt_run_and_test(self, model: str, upstream: list = None):
        dbt_profile = self.get_dbt_profile()
        cmd_run = f"dbt --no-write-json run --models {model}"
        cmd_test = f"dbt --no-write-json test --models {model}"
        upstream_tasks = upstream or []
        future_run = trigger_dbt_cli_command.with_options(
            name=f"{self.default_dbt_model_emoji}{model}",
            retries=self.retries,
            retry_delay_seconds=self.retry_delay_seconds,
        ).submit(
            cmd_run,
            project_dir=self.path_to_dbt_project,
            dbt_cli_profile=dbt_profile,
            overwrite_profiles=True,
            wait_for=upstream_tasks,
        )
        state_run = future_run.wait()
        future_test = trigger_dbt_cli_command.with_options(
            name=f"{self.default_dbt_test_emoji}test {model}",
            retries=self.retries,
            retry_delay_seconds=self.retry_delay_seconds,
        ).submit(
            cmd_test,
            project_dir=self.path_to_dbt_project,
            dbt_cli_profile=dbt_profile,
            overwrite_profiles=True,
            wait_for=[future_run, *upstream_tasks],
        )
        state_test = future_test.wait()
        alert_run = cmd_run.replace(" --no-write-json", "")
        alert_test = cmd_test.replace(" --no-write-json", "")
        self.workspace.send_alert_on_failure(state_run, alert_run)
        self.workspace.send_alert_on_failure(state_test, alert_test)
        return future_test

    def dbt_run_from_manifest(self):
        with open(f"{self.path_to_dbt_project}/target/manifest.json") as f:
            data = json.load(f)

        already_executed_models = []
        already_executed_tasks = {}
        for node in data["nodes"].keys():
            node_type = node.split(".")[0]
            model = node.split(".")[-1]
            if node_type == "model":
                upstream = data["nodes"][node]["depends_on"]["nodes"]
                # filter out sources and leave only models
                upstream_nodes = [
                    i.split(".")[-1] for i in upstream if i.split(".")[0] == "model"
                ]
                if upstream_nodes:
                    correct_upnodes = []
                    for upstream_model in upstream_nodes:
                        if upstream_model not in already_executed_models:
                            dbt_task = self._dbt_run_and_test(upstream_model)
                            correct_upnodes.append(dbt_task)
                            already_executed_tasks[
                                upstream_model
                            ] = dbt_task  # for lineage
                            already_executed_models.append(upstream_model)
                        else:
                            correct_upnodes.append(
                                already_executed_tasks[upstream_model]
                            )
                    self._dbt_run_and_test(model, correct_upnodes)
