maintenance_flow = "flows/orchestration/maintenance.py:maintenance"
main_flows = [
    "flows/orchestration/jaffle_shop/ingest_transform.py:jaffle_shop_ingest_transform",
    "flows/orchestration/attribution/ingest_transform.py:attribution_ingest_transform",
    "flows/orchestration/run_deployments/parent.py:parent",
]
ingestion_flows = [
    # Optionally, also create deployments for the ingestion flows
    "flows/ingestion/ingest_jaffle_shop.py:raw_data_jaffle_shop",
    "flows/ingestion/ingest_marketing_data.py:raw_data_marketing",
]
dbt_transformation_flows = [
    # Optionally, also create deployments for the transformation flows
    "flows/transformation/jaffle_shop/dbt_run_from_manifest.py:dbt_jaffle_shop",
    "flows/transformation/attribution/dbt_run_from_manifest.py:dbt_attribution",
]
simple_dbt_parametrized = [
    "flows/transformation/jaffle_shop/dbt_build.py:dbt_jaffle_shop",
    "flows/transformation/attribution/dbt_build.py:dbt_attribution",
]
dbt_from_repo = [
    "flows/transformation/jaffle_shop/dbt_repo.py:dbt_jaffle_shop",
    "flows/transformation/attribution/dbt_repo.py:dbt_attribution",
]
ingestion_subflows_marketing = [
    # Optionally, add deployments for raw marketing data subflows, to allow running those individual flows ad-hoc
    "flows/ingestion/ingest_marketing_data.py:raw_ad_spend",
    "flows/ingestion/ingest_marketing_data.py:raw_sessions_and_conversions",
]
analytics = ["flows/analytics/dashboards.py:dashboards"]
ml = ["flows/ml/sales_forecast.py:sales_forecast"]
