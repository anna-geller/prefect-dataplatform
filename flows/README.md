# Data Platform Flows

## Ingestion
Flows that extract raw data from source systems into the staging area of your data warehouse.

## Transformation
Flows that perform in-warehouse transformations using ``dbt``

## Orchestration
Parent flows that trigger all ingestion, transformation and downstream flows in the right order. Only flows from this directory need to have deployments and need to run on schedule. 


![](../utilities/bot.jpeg)
