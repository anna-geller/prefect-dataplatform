prefect deployment run jaffle-shop-ingest-transform/default
prefect deployment run attribution-ingest-transform/default

prefect deployment run raw-data-jaffle-shop/default
prefect deployment run raw-data-marketing/default

prefect deployment run dbt-cloned-repo/default
prefect deployment run dbt-jaffle-shop/default
prefect deployment run dbt-jaffle-shop/simple

prefect deployment run dbt-attribution/default
prefect deployment run dbt-attribution/simple

prefect deployment run raw-ad-spend/default
prefect deployment run raw-sessions-and-conversions/default
