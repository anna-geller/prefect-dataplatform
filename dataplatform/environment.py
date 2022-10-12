import asyncio
from prefect.cli.cloud import get_current_workspace
from prefect.client.cloud import get_cloud_client, CloudUnauthorizedError


async def get_workspace() -> str:
    try:
        async with get_cloud_client() as client:
            workspaces = await client.read_workspaces()
            current_workspace = get_current_workspace(workspaces)
            workspace_handle = current_workspace.split("/")[-1]
            return workspace_handle
    except CloudUnauthorizedError:
        return "default"  # means: local Orion instance and the default CLI Profile


def get_env() -> str:
    """
    This could be replaced by some other logic to return whether you run sth in dev vs. prod
    :return: string representing the environment, same as assigned to block names
    """
    return asyncio.run(get_workspace())
