"""
Note: this K8s manifest requires a Prefect Cloud workspace, for OSS K8s deployment, consider https://github.com/PrefectHQ/prefect-helm
prefect kubernetes manifest agent --work-queue default --image-tag prefecthq/prefect:2-python3.9 --namespace default | kubectl apply --namespace=default -f -
"""
from prefect.infrastructure import KubernetesJob

from dataplatform.deploy_utils import build_image, save_block, bash
import flows.entrypoints_config as cfg

deploy_agent = True
image_agent = "prefecthq/prefect:2-python3.9"
k8s_namespace = "default"
image_name = "dataplatform"
block_name = "default"
build = "prefect deployment build"
name = "k8s-s3"
sb = f"-sb s3/{block_name}"
su = "--skip-upload"  # upload only for the maintenance flow, skip upload for the rest
ib = f"-ib kubernetes-job/{block_name}"
queue_name = "k8s"
wq = f"-q {queue_name}"


if __name__ == "__main__":
    bash("python utilities/create_blocks.py")
    if deploy_agent:
        k8s_cmd = "prefect kubernetes manifest agent"
        bash(
            f"{k8s_cmd} --work-queue {queue_name}  --image-tag {image_agent} --namespace {k8s_namespace} | kubectl apply --namespace={k8s_namespace} -f -",
        )
    image_sha = build_image(image_name)
    block = KubernetesJob(
        finished_job_ttl=300,
        image=image_sha,
        namespace=k8s_namespace,
        image_pull_policy="Never",
        env={"PREFECT_LOGGING_LEVEL": "INFO"},
    )
    save_block(block, block_name)

    # Deploy FLOWS
    bash(f"{build} {ib} {sb} {wq} -n {name} {cfg.maintenance_flow} -a")

    for flow in cfg.main_flows:
        tags = "-t parent"
        bash(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.ingestion_flows:
        tags = "-t ingestion"
        bash(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.ingestion_subflows_marketing:
        tags = "-t ingestion -t Marketing"
        bash(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.dbt_transformation_flows:
        tags = "-t dbt"
        bash(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.simple_dbt_parametrized:
        tags = "-t dbt"
        bash(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n simple-{name} -a")

    for flow in cfg.dbt_from_repo:
        tags = "-t dbt"
        bash(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n dbt-repo-{name} -a")

    for flow in cfg.analytics:
        tags = "-t Analytics"
        bash(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a")

    for flow in cfg.ml:
        tags = "-t ML"
        bash(f"{build} {ib} {sb} {su} {wq} {flow} {tags} -n {name} -a")
