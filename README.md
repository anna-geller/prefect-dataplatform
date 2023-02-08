# Data Platform with Prefect, dbt and Snowflake
Example repository showing how to leverage Prefect, dbt and Snowflake together to build a data platform. This project includes automation to make it easy to use this repositor as a template to get started, and adjust it based on the needs of your team.

## Blog posts explaining how to use this repo

[How to Build a Modular Data Stack — Data Platform with Prefect, dbt and Snowflake](https://medium.com/the-prefect-blog/how-to-build-a-modular-data-stack-data-platform-with-prefect-dbt-and-snowflake-89f928974e85)

# Dbt commands

The following flows are parametrized and allow you to easily trigger any dbt command (both locally and from the Prefect UI):

- [flows/transformation/jaffle_shop/dbt_build.py](flows/transformation/jaffle_shop/dbt_build.py)
- [flows/transformation/attribution/dbt_build.py](flows/transformation/attribution/dbt_build.py)

## Summary of `dbt` commands you can provide

- `dbt debug` = test connection and profile
- `dbt compile` = compile `target/manifest.json` file
- `dbt deps` = install dependencies
- `dbt seed` = ingest raw data from seed files
- `dbt run` = run all models
- `dbt run --select xyz` = run selected model(s)
- `dbt test` = run tests
- `dbt build` = run seeds, models, tests, snapshots, in a DAG order
- ``dbt build --select result:error+ --defer --state ./target`` - use the `--select` flag together with `--defer` and `--state` to **manually** rerun your dbt DAG from a failed node
- ``dbt run --select failed_model+`` - if you know which model failed and you want to directly trigger a run from that failed model and all downstream dependencies


# How to run flow scripts locally

The Prefect task ``prefect_dbt.cli.commands.trigger_dbt_cli_command`` requires that the dbt `project_dir` is located either in the root project directory, or in a path that you explicitly provide.

To avoid any issues with paths, it's best to run the flow as a script, rather than from iPython. Examples:

```
python flows/transformation/attribution/dbt_build.py
python flows/transformation/attribution/dbt_run_from_manifest.py
python flows/transformation/jaffle_shop/dbt_build.py
python flows/transformation/jaffle_shop/dbt_cloned_repo.py
python flows/transformation/jaffle_shop/dbt_run_from_manifest.py
```

![](utilities/dbt_bot.jpeg)


# How to create deployments

You can create deployments from the automated scripts.

Note that you can add tags and schedule directly from the UI, so we deliberately skipped that in the CLI commands. 

## Running flows in a local process

[deploy_locally.py](deploy_locally.py)

```bash
python deploy_locally.py
```

## Running flows in a Docker container

[deploy_locally_docker.py](deploy_locally_docker.py)

```bash
python deploy_locally_docker.py
```

## Running flows in a Kubernetes job

[deploy_locally_k8s.py](deploy_locally_k8s.py)

```bash
python deploy_locally_k8s.py
```

## Deploy to AWS with Docker and S3

[deploy_docker_s3.py](deploy_docker_s3.py)

```bash
python deploy_docker_s3.py
```

## Deploy to AWS with Kubernetes and S3

[deploy_k8s_s3.py](deploy_k8s_s3.py)

```bash
python deploy_k8s_s3.py
```

# Dev vs. Prod environments

The entire repository is built so that you can reuse the same code for development and production environment. No environment information is hardcoded here:

- all blocks and work-queues are by default created with the name ``default`` so that differentiating between dev and prod environments is as easy as pointing to the `development` or `production` Prefect Cloud workspace 
- if you are on the OSS version, you can accomplish the same by pointing to a different ``PREFECT_API_URL`` of your `dev` vs. `prod` Orion API server instances (the assumption is that those are separate servers)
- for both Prefect Cloud and self-hosted Orion, all credentials can be securely stored using blocks to avoid reliance on environment variables baked into your execution environment, or hardcoded configuration values.

> 🧠 _"Switching between `dev`, `staging` and `prod` environments, various infrastructures, storage, and cloud data warehouses was never easier. CI/CD became almost enjoyable!"_ - Marvin

## `Infrastructure` and `Storage` blocks are hot-swappable
To switch the above setup from S3 to GCS or Azure Blob Storage, it's a matter of pointing to a different storage block. All blocks are defined in [utilities/create_blocks.py](utilities/create_blocks.py).

# Adding schedules

## From CLI
To add schedule to deployments, you can do it using the command ``prefect deployment set-schedule`` and by referencing the deployment in the format `flow_name/deployment_name`:
```bash
prefect deployment set-schedule jaffle-shop-ingest-transform/default --cron "0 * * * *"
prefect deployment set-schedule attribution-ingest-transform/default --cron "0 * * * *"
# For more options, check:
prefect deployment set-schedule --help
```

You can also attach schedules directly to the ``prefect deployment build`` if you prefer to do it that way. You can add schedule using either `--cron`, `--interval` or `--rrule` flags:

```bash
prefect deployment build -q default -n default -a flows/ingestion/ingest_jaffle_shop.py:raw_data_jaffle_shop --interval 3600
# For more options, check:
prefect deployment build --help
```

## From the UI

> 💡 The easiest way to attach a schedule to a deployment is to do it directly from the UI. Go to the deployment UI page and add schedule from there. 

The [following documentation page](https://docs.prefect.io/concepts/schedules/) provides more information about that.


# How to trigger ad-hoc run from a deployment?

Use the command ``prefect deployment run`` and reference your deployment in the format `flow_name/deployment_name`:
```bash
prefect deployment run jaffle-shop-ingest-transform/default
prefect deployment run attribution-ingest-transform/default
# Start an agent to create runs from deployments
prefect agent start -q default
```

The last line starts an agent polling from work-queue `default`. You don't have to create a work queue -- Prefect will create it automatically if it doesn't exist yet. 

Commands to run all flows from a deployment via CLI are available in [utilities/run_deployments_from_cli.bash](utilities/run_deployments_from_cli.bash).

# Troubleshooting

## I got a `RuntimeError`: ``fatal: Invalid --project-dir flag. Not a dbt project. Missing dbt_project.yml file``

This means that either:
- the ``project_dir`` doesn't correctly point to your dbt project -- make sure that you provide correct path (relative or absolute), alternatively, put the dbt project into your project's root directory -- this way, the ``trigger_dbt_cli_command`` task will be able to find it directly
- when your agent deploys flow to some custom infrastructure, it copies the files from your remote storage into a /tmp directory for the flow run execution; it might be that some relative path to your dbt project directory doesn't work the same way when running within the agent's environment -- you can avoid that by either having your dbt project in the Prefect's project root directory, or by using absolute paths.

## How to switch between `dev` and `prod` from CLI?

You can leverage Prefect CLI profiles to easily switch between environments. 

### Create development workspace and profile

Go to the UI and create a workspace if you don't have one already.
Then create an API key and use that to fill the information below.
```bash
prefect profile create dev
prefect cloud workspace set --workspace "your_prefect_cloud_account/dev"
prefect config set PREFECT_API_KEY=pnu_topsecret123456789
```

### Create production workspace and profile

Now do the same for production. 
```bash
prefect profile create prod
prefect cloud workspace set --workspace "your_prefect_cloud_account/prod"
prefect config set PREFECT_API_KEY=this_must_be_a_different_api_key_as_they_are_bound_to_workspace
```



# Benefits of the approach shown in this repository

- no hardcoding of credentials or environment information: when you are in the `dev` workspace, everything works the same way as in `prod` but based on your `dev` credentials; you can move to production with no modifications to your code simply by switching to a `prod` workspace 
- leveraging blocks to make it easy to change variables (such as dbt project path, number of retries, even emojis for your dbt tasks), 
- easily rotate credentials, 
- track metadata and capabilities of each block, 
- reuse of components across teams while still being able to easily take the same block logic but apply it in a different setting simply by creating another block of the same type and pointing to that new block name in your workflow logic (see the distinction between dbt block for jaffle_shop and attribution in [this file](utilities/create_blocks.py))
- switching between execution in a local process, execution in a docker container, or execution in a Kubernetes cluster, is as simple as switching the infrastructure block in your deployment CLI command
- switching from local execution on your machine to `dev` and `production` environment is as simple as pointing to a different workspace and CLI profile
- simple dbt flows are **parametrized** to make it easy to execute any dbt command for the relevant project
- all ingestion flows are parametrized to make **backfills** a breeze - just change the start and end date, and the interval in your parameter values when triggering the run, and you can trigger a backfill from the same flow as usual -- see the flow parameters e.g. in the flow [flows/ingestion/ingest_jaffle_shop.py](flows/ingestion/ingest_jaffle_shop.py)
- optionally, you can track each dbt model or test as a separate task by using the ``dbt_run_from_manifest.py`` logic -- [dataplatform/blocks/dbt.py](dataplatform/blocks/dbt.py)
- it's fun to use! if you want to change how your dbt tasks look like for Halloween or Christmas, change the emoji on the block 🤗
