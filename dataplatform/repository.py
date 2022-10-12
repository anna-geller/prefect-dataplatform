"""
Example usage:
pull_repo(repo="jaffle_shop", org="anna-geller")
"""
import asyncio
import shutil
from prefect.filesystems import GitHub


def pull_repo(repo: str, org: str, path: str = None) -> None:
    # private repo: https://${PERSONAL_ACCESS_TOKEN}@github.com/org/repo.git
    gh = GitHub(repository=f"https://github.com/{org}/{repo}.git")
    path_ = path or f"../{repo}"
    shutil.rmtree(path_, ignore_errors=True)
    asyncio.run(gh.get_directory(local_path=path_))
