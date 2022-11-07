Imagine that this flow would require a dedicated GPU. 
Here is how you could request GPU resources for such ML job on a cloud K8s cluster such as on GKE. 

## Create the infrastructure block

First, create a dedicted K8s infra block e.g. for [GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/gpus):

```python
from prefect.infrastructure import KubernetesJob

k8s_job = KubernetesJob(
    namespace="prefect",
    customizations=[
        {
            "op": "add",
            "path": "/spec/template/spec/resources",
            "value": {"limits": {}},
        },
        {
            "op": "add",
            "path": "/spec/template/spec/resources/limits",
            "value": {"nvidia.com/gpu": 2},
        },
        {
            "op": "add",
            "path": "/spec/template/spec/nodeSelector",
            "value": {"cloud.google.com/gke-accelerator": "nvidia-tesla-k80"},
        },
    ],
)
k8s_job.save("gpu", overwrite=True)
```

To do something similar on AWS EKS, you could leverage [Karpenter](https://karpenter.sh/v0.6.1/aws/provisioning/#accelerators-gpu).

## Create a deployment with that infrastructure block
Then, create a deployment with that infra block:

```bash
prefect deployment build -ib kubernetes-job/gpu -sb s3/default -n gpu -q default -a flows/ml/sales_forecast.py:sales_forecast
```

## Run that deployment e.g. from a parent flow

Trigger a run from that deployment:
```python
from prefect.deployments import run_deployment

run_deployment(name="sales-forecast/gpu", flow_run_name="ML on K8s with GPU")
```

