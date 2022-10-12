"""
Note: this K8s manifest requires a Prefect Cloud workspace, for OSS K8s deployment, consider https://github.com/PrefectHQ/prefect-helm
prefect kubernetes manifest agent --work-queue default --image-tag prefecthq/prefect:2-python3.9 --namespace default | kubectl apply --namespace=default -f -
"""
import subprocess
import flows.entrypoints_config as cfg
from dataplatform.deploy_utils import build_image, save_block
from prefect.infrastructure import KubernetesJob

deploy_agent = True
image_agent = "prefecthq/prefect:2-python3.9"
k8s_namespace = "default"
image_name = "dataplatform"
block_name = "default"
name = "k8s"
ib = f"-ib kubernetes-job/{block_name}"
build = "prefect deployment build"
queue_name = "k8s"
wq = f"-q {queue_name}"


if __name__ == "__main__":
    subprocess.run("python utilities/create_blocks.py", shell=True)
    if deploy_agent:
        k8s_cmd = "prefect kubernetes manifest agent"
        subprocess.run(
            f"{k8s_cmd} --work-queue {queue_name}  --image-tag {image_agent} --namespace {k8s_namespace} | kubectl apply --namespace={k8s_namespace} -f -",
            shell=True,
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
    subprocess.run(f"{build} {ib} {wq} -n {name} {cfg.maintenance_flow} -a", shell=True)

    for flow in cfg.main_flows:
        tags = "-t parent"
        subprocess.run(f"{build} {ib} {wq} {flow} {tags} -n {name} -a", shell=True)

    for flow in cfg.ingestion_flows:
        tags = "-t ingestion"
        subprocess.run(f"{build} {ib} {wq} {flow} {tags} -n {name} -a", shell=True)

    for flow in cfg.ingestion_subflows_marketing:
        tags = "-t ingestion -t Marketing"
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
