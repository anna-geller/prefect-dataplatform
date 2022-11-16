from datetime import date
import numpy as np
import pandas as pd
from prefect import flow, task

from flows.ingestion.marketing_config import Frequency, campaigns, channels
from dataplatform.blocks.snowflake_pandas import SnowflakePandas


@task(retries=3, retry_delay_seconds=30)
def add_utc_source_and_campaign(df: pd.DataFrame) -> pd.DataFrame:
    rows = len(df)
    df["utm_source"] = np.random.choice(channels, size=rows)
    df["utm_campaign"] = np.random.choice(campaigns, size=rows)
    return df


@task(retries=3, retry_delay_seconds=30)
def clean_utm_campaigns(df: pd.DataFrame) -> pd.DataFrame:
    no_campaigns = ["google", "bing", "direct", "newsletter"]
    df["utm_campaign"] = np.where(
        df["utm_source"].isin(no_campaigns), "", df["utm_campaign"]
    )
    df["utm_campaign"] = np.where(
        df["utm_source"].isin(["adwords"]), "branded_search", df["utm_campaign"]
    )
    df["utm_campaign"] = np.where(
        df["utm_source"].isin(["direct"]), "", df["utm_campaign"]
    )
    return df


@task(retries=3, retry_delay_seconds=30)
def clean_utm_medium(df: pd.DataFrame) -> pd.DataFrame:
    df["utm_medium"] = np.where(
        df["utm_source"].isin(["google", "bing"]), "search", "paid_social"
    )
    df["utm_medium"] = np.where(
        df["utm_source"].isin(["newsletter"]), "email", df["utm_medium"]
    )
    df["utm_medium"] = np.where(
        df["utm_source"].isin(["adwords"]), "paid_search", df["utm_medium"]
    )
    df["utm_medium"] = np.where(df["utm_source"].isin(["direct"]), "", df["utm_medium"])
    return df


@task(retries=3, retry_delay_seconds=30)
def load(df: pd.DataFrame, table_name: str) -> None:
    block = SnowflakePandas.load("default")
    block.load_raw_data(df, table_name)


@flow(retries=3, retry_delay_seconds=30)
def raw_ad_spend(
        start_date: date = date(2020, 2, 1),
        end_date: date = date.today(),
        interval: Frequency = "D",
) -> None:
    df = pd.DataFrame(
        pd.date_range(start=start_date, end=end_date, freq=interval),
        columns=["date_day"],
    )
    df["date_day"] = df["date_day"].astype(str)
    rows = len(df)
    df = add_utc_source_and_campaign(df)
    df["spend"] = np.random.randint(1, 99, size=rows)
    df = clean_utm_campaigns(df)
    df = clean_utm_medium(df)
    df = df[["date_day", "utm_source", "utm_medium", "utm_campaign", "spend"]]
    load(df, "raw_ad_spend")


@flow(retries=3, retry_delay_seconds=30)
def raw_sessions_and_conversions(
        start_date: date = date(2020, 2, 1),
        end_date: date = date.today(),
        interval: Frequency = "D",
) -> None:
    df = pd.DataFrame(
        pd.date_range(start=start_date, end=end_date, freq=interval),
        columns=["started_at"],
    )
    rows = len(df)
    td = pd.to_timedelta(np.random.randint(1, 120, size=rows), unit="s")
    df["ended_at"] = df["started_at"] + td
    df["session_id"] = df.index + 1
    df["customer_id"] = np.random.randint(1, 4200, size=rows)
    df = add_utc_source_and_campaign(df)
    df = clean_utm_campaigns(df)
    df = clean_utm_medium(df)
    load(df, "raw_sessions")
    conv = (
        df[["customer_id", "ended_at", "session_id"]]
            .groupby(["customer_id"], as_index=False)
            .agg(converted_at=("ended_at", "max"), revenue=("session_id", "count"))
    )
    load(conv, "raw_customer_conversions")


@flow(retries=3, retry_delay_seconds=30)
def raw_data_marketing(
        start_date: date = date(2020, 2, 1),  # parametrized for backfills
        end_date: date = date.today(),
        interval: Frequency = "D",
):
    raw_ad_spend(start_date, end_date, interval)
    raw_sessions_and_conversions(start_date, end_date, interval)


if __name__ == "__main__":
    raw_data_marketing()
