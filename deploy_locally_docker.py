from prefect.infrastructure import DockerContainer

from dataplatform.deploy_utils import build_image, save_block, bash
import flows.entrypoints_config as cfg

image_name = "dataplatform"
block_name = "default"
build = "prefect deployment build"
name = "docker"
ib = f"-ib docker-container/{block_name}"
queue_name = "default"
wq = f"-q {queue_name}"


if __name__ == "__main__":
    bash("python utilities/create_blocks.py")
    image_sha = build_image(image_name)
    block = DockerContainer(image=image_sha, image_pull_policy="NEVER")
    save_block(block, block_name)

    # Deploy FLOWS
    bash(f"{build} {ib} {wq} -n {name} {cfg.maintenance_flow} -a")

    for flow in cfg.main_flows:
        tags = "-t parent"
        bash(f"{build} {ib} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.ingestion_flows:
        tags = "-t ingestion"
        bash(f"{build} {ib} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.ingestion_subflows_marketing:
        tags = "-t ingestion -t Marketing"
        bash(f"{build} {ib} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.dbt_transformation_flows:
        tags = "-t dbt"
        bash(f"{build} {ib} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.simple_dbt_parametrized:
        tags = "-t dbt"
        bash(f"{build} {ib} {wq} {flow} {tags} -n simple-{name} -a")

    for flow in cfg.dbt_from_repo:
        tags = "-t dbt"
        bash(f"{build} {ib} {wq} {flow} {tags} -n dbt-repo-{name} -a")

    for flow in cfg.analytics:
        tags = "-t Analytics"
        bash(f"{build} {ib} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.ml:
        tags = "-t ML"
        bash(f"{build} {ib} {wq} {flow} {tags} -n {name} -a")
