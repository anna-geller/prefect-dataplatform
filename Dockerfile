FROM prefecthq/prefect:2-python3.10
COPY flows /opt/prefect/flows
COPY dbt_attribution /opt/prefect/dbt_attribution
COPY dbt_jaffle_shop /opt/prefect/dbt_jaffle_shop
COPY dataplatform /opt/prefect/dataplatform
COPY requirements.txt /opt/prefect/requirements.txt
COPY setup.py /opt/prefect/setup.py
RUN pip install . --no-cache-dir