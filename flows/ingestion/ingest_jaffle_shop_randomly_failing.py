from datetime import date, timedelta
from faker import Faker
import pandas as pd
from prefect import flow, task
import random

from dataplatform.blocks.snowflake_pandas import SnowflakePandas

fake = Faker()


def random_date(
        start_date: date = date(2020, 2, 1), end_date: date = date.today()
) -> str:
    delta = end_date - start_date
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return str(start_date + timedelta(seconds=random_second))


@task  # (retries=3, retry_delay_seconds=30)
def ingest_raw_payments(sample_size=100):
    if random.random() > 0.5:
        raise ValueError("Non-deterministic error has occured.")
    else:
        output = [
            {
                "id": random.randint(1, sample_size),
                "order_id": random.randint(1, sample_size),
                "payment_method": random.choice(
                    ["credit_card", "coupon", "bank_transfer", "gift_card"]
                ),
                "amount": random.randint(1, 5000),
            }
            for x in range(sample_size)
        ]
        df = pd.DataFrame(output)
        df = df.drop_duplicates(subset=["id"])
        load(df, "raw_payments")


@task(retries=3, retry_delay_seconds=30)
def ingest_raw_orders(
        sample_size=100, start_date: date = date(2020, 2, 1), end_date: date = date.today()
):
    output = [
        {
            "id": random.randint(1, sample_size),
            "user_id": random.randint(1, sample_size),
            "order_date": random_date(start_date, end_date),
            "status": random.choice(
                ["returned", "completed", "return_pending", "shipped", "placed"]
            ),
        }
        for x in range(sample_size)
    ]
    df = pd.DataFrame(output)
    df = df.drop_duplicates(subset=["id"])
    load(df, "raw_orders")


@task(retries=3, retry_delay_seconds=30)
def ingest_raw_customers(sample_size=100):
    output = [
        {
            "id": random.randint(1, sample_size),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
        }
        for x in range(sample_size)
    ]
    df = pd.DataFrame(output)
    df = df.drop_duplicates(subset=["id"])
    load(df, "raw_customers")


def load(df: pd.DataFrame, table_name: str) -> None:
    block = SnowflakePandas.load("default")
    block.load_raw_data(df, table_name)


@flow  # (retries=3, retry_delay_seconds=30)
def raw_data_jaffle_shop(
        start_date: date = date(2020, 2, 1),
        end_date: date = date.today(),
        dataset_size: int = 10_000,  # parametrized for backfills
):
    ingest_raw_customers.submit(dataset_size)
    ingest_raw_payments.submit(dataset_size)
    ingest_raw_orders.submit(dataset_size, start_date, end_date)


if __name__ == "__main__":
    raw_data_jaffle_shop(dataset_size=1_000)  # override when needed
