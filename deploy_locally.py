from prefect.infrastructure import Process

from dataplatform.deploy_utils import save_block, bash
import flows.entrypoints_config as cfg

name = "local-process"
queue_and_blocks_name = "default"
ib = f"-ib process/{queue_and_blocks_name}"
build = "prefect deployment build"
wq = f"-q {queue_and_blocks_name}"


if __name__ == "__main__":
    bash("python utilities/create_blocks.py")

    process_block = Process(env={"PREFECT_LOGGING_LEVEL": "DEBUG"})
    save_block(process_block, queue_and_blocks_name)

    bash(f"{build} {ib} {wq} -n {name} {cfg.maintenance_flow} -a")

    # Deploy FLOWS
    for flow in cfg.main_flows:
        tags = "-t parent"
        bash(f"{build} {ib} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.ingestion_flows:
        tags = "-t Ingestion"
        bash(f"{build} {ib} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.ingestion_subflows_marketing:
        tags = "-t Ingestion -t Marketing"
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
