import subprocess
import flows.entrypoints_config as cfg
from dataplatform.deploy_utils import build_image, save_block
from prefect.infrastructure import DockerContainer

image_name = "dataplatform"
block_name = "default"
build = "prefect deployment build"
name = "docker-s3"
sb = f"-sb s3/{block_name}"
su = "--skip-upload"  # upload only for the maintenance flow, skip upload for the rest
ib = f"-ib docker-container/{block_name}"
queue_name = "default"
wq = f"-q {queue_name}"


if __name__ == "__main__":
    subprocess.run("python utilities/create_blocks.py", shell=True)
    image_sha = build_image(image_name)
    block = DockerContainer(image=image_sha, image_pull_policy="NEVER")
    save_block(block, block_name)

    # Deploy FLOWS
    subprocess.run(f"{build} {ib} {sb} {wq} -n {name} {cfg.maintenance_flow} -a", shell=True)

    for flow in cfg.main_flows:
        tags = "-t parent"
        subprocess.run(
            f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a", shell=True
        )

    for flow in cfg.ingestion_flows:
        tags = "-t ingestion"
        subprocess.run(
            f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a", shell=True
        )

    for flow in cfg.ingestion_subflows_marketing:
        tags = "-t ingestion -t Marketing"
        subprocess.run(
            f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a", shell=True
        )

    for flow in cfg.dbt_transformation_flows:
        tags = "-t dbt"
        subprocess.run(
            f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a", shell=True
        )

    for flow in cfg.simple_dbt_parametrized:
        tags = "-t dbt"
        subprocess.run(
            f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n simple-{name} -a", shell=True
        )

    for flow in cfg.analytics:
        tags = "-t Analytics"
        subprocess.run(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a", shell=True)

    for flow in cfg.ml:
        tags = "-t ML"
        subprocess.run(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a", shell=True)
