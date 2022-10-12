import subprocess
import flows.entrypoints_config as cfg
from prefect.infrastructure import Process
from dataplatform.deploy_utils import save_block

name = "local-process"
queue_and_blocks_name = "default"
ib = f"-ib process/{queue_and_blocks_name}"
build = "prefect deployment build"
wq = f"-q {queue_and_blocks_name}"


if __name__ == "__main__":
    subprocess.run("python utilities/create_blocks.py", shell=True)
    process_block = Process(env={"PREFECT_LOGGING_LEVEL": "DEBUG"})
    save_block(process_block, queue_and_blocks_name)
    subprocess.run(f"{build} {ib} {wq} -n {name} {cfg.maintenance_flow} -a", shell=True)

    # Deploy FLOWS
    for flow in cfg.main_flows:
        tags = "-t parent"
        subprocess.run(f"{build} {ib} {wq} {flow} {tags} -n {name} -a", shell=True)

    for flow in cfg.ingestion_flows:
        tags = "-t Ingestion"
        subprocess.run(f"{build} {ib} {wq} {flow} {tags} -n {name} -a", shell=True)

    for flow in cfg.ingestion_subflows_marketing:
        tags = "-t Ingestion -t Marketing"
        subprocess.run(f"{build} {ib} {wq} {flow} {tags} -n {name} -a", shell=True)

    for flow in cfg.dbt_transformation_flows:
        tags = "-t dbt"
        subprocess.run(f"{build} {ib} {wq} {flow} {tags} -n {name} -a", shell=True)

    for flow in cfg.simple_dbt_parametrized:
        tags = "-t dbt"
        subprocess.run(
            f"{build} {ib} {wq} {flow} {tags} -n simple-{name} -a", shell=True
        )

    for flow in cfg.analytics:
        tags = "-t Analytics"
        subprocess.run(f"{build} {ib} {wq} {flow} {tags} -n {name} -a", shell=True)

    for flow in cfg.ml:
        tags = "-t ML"
        subprocess.run(f"{build} {ib} {wq} {flow} {tags} -n {name} -a", shell=True)
