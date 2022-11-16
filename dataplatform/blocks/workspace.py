from prefect.blocks.core import Block
from prefect.blocks.notifications import SlackWebhook
from prefect.settings import PREFECT_API_URL
from pydantic import Field
from typing import Any
from uuid import UUID


class Workspace(Block):

    """
    Manage alerts and workspace metadata

    Args:
        name: environment name e.g. dev, staging, prod
        metadata: key-value pairs representing workspace information

    Example:
        Load a stored JSON value:
        ```python
        from dataplatform.blocks import Workspace

        workspace = Workspace.load("BLOCK_NAME")
        workspace.send_alert("Alert from Prefect! 🚀")
        ```
    """

    _block_type_name = "Workspace"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/19W3Di10hhb4oma2Qer0x6/764d1e7b4b9974cd268c775a488b9d26/image16.png?h=250"
    _block_schema_capabilities = ["send_alert", "send_alert_on_failure"]

    name: str = Field(default="dev", description="The name of your workspace.")
    block_name: str = Field(
        default="default",
        description="Block name used for default blocks bound to this workspace",
    )
    metadata: Any = Field(
        default=dict(workspace_owner="Data Team"),
        description="A JSON-compatible field for storing additional workspace settings.",
    )

    def send_alert(self, message: str) -> None:
        webhook = SlackWebhook.load(self.block_name)
        webhook.notify(message)

    @staticmethod
    def _get_task_run_page_url(task_run_id: UUID) -> str:
        """
        Returns a link to the task run page.
        Args:
            task_run_id: the task run id.
        """
        api_url = PREFECT_API_URL.value() or "http://ephemeral-orion/api"
        ui_url = (
            api_url.replace("api", "app")
            .replace("app/accounts", "account")
            .replace("workspaces", "workspace")
        )
        return f"{ui_url}/flow-runs/task-run/{task_run_id}"

    def send_alert_on_failure(self, state, failure_reason: str = None):
        task_run_id = state.state_details.task_run_id
        flow_run_id = state.state_details.flow_run_id
        reason = failure_reason or f"Flow run {flow_run_id}"
        url = self._get_task_run_page_url(task_run_id)
        if state.name == "Failed":
            self.send_alert(f"`{reason}` failed. Details: {url}")
