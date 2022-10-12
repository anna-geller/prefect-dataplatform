prefect deployment build -q default -n default -a -t parent flows/orchestration/jaffle_shop/ingest_transform.py:jaffle_shop_ingest_transform
prefect deployment build -q default -n default -a -t parent flows/orchestration/attribution/ingest_transform.py:attribution_ingest_transform
# -----------------------------------------------------------------------------------
# Optionally, also create deployments for the ingestion flows
prefect deployment build -q default -n default -a flows/ingestion/ingest_jaffle_shop.py:raw_data_jaffle_shop
prefect deployment build -q default -n default -a flows/ingestion/ingest_marketing_data.py:raw_data_marketing

# Optionally, also create deployments for the transformation jobs
prefect deployment build -q default -n simple -a flows/transformation/jaffle_shop/dbt_build.py:dbt_jaffle_shop
prefect deployment build -q default -n default -a flows/transformation/jaffle_shop/dbt_cloned_repo.py:dbt_cloned_repo
prefect deployment build -q default -n default -a flows/transformation/jaffle_shop/dbt_run_from_manifest.py:dbt_jaffle_shop

prefect deployment build -q default -n simple -a flows/transformation/attribution/dbt_build.py:dbt_attribution
prefect deployment build -q default -n default -a flows/transformation/attribution/dbt_run_from_manifest.py:dbt_attribution
# -----------------------------------------------------------------------------------
# Optionally, adding deployments for subflows in the raw marketing data, to allow running those individual flows ad-hoc
prefect deployment build -q default -n default -a flows/ingestion/ingest_marketing_data.py:raw_ad_spend
prefect deployment build -q default -n default -a flows/ingestion/ingest_marketing_data.py:raw_sessions_and_conversions
