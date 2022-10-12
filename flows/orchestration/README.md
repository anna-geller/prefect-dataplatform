# Only flows from this directory need to run on schedule 
Technically, all flows from this repository can be orchestrated either by running them locally or by creating a deployment (and optionally, attaching a schedule to it).

The workflows from this directory are meant to be used for the regular data warehouse refresh process and should be scheduled. In contrast, flows from the ``ingestion`` and ``transformation`` directories don't need to be scheduled, and don't strictly need a deployment, because they are triggered from parent flows in this directory. 

For instance, the ``flows/orchestration/jaffle_shop/ingest_transform.py`` already calls the following flows as subflows: 
- ingestion flow:  ``flows/ingestion/ingest_jaffle_shop.py``
- transformation flow:  ``flows/transformation/jaffle_shop/dbt_run_from_manifest.py.py``

This means that these two flows don't need to have a deployment and they don't need to run on schedule -- only the parent flow needs to run on a regular cadence and it will already trigger child flows when needed.
