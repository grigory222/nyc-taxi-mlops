import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import requests

DB_URL = "postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db"
TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"


def download_and_load(year: int, month: int) -> None:
    url = TAXI_URL.format(year=year, month=month)
    print(f"Скачиваем {url}...")

    df = pd.read_parquet(url)

    df = df[[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
    ]].copy()

    df["duration_min"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_dayofweek"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["is_weekend"] = df["pickup_dayofweek"].isin([5, 6]).astype(int)
    df["month"] = month
    df["year"] = year

    df = df[
        (df["duration_min"] >= 1) &
        (df["duration_min"] <= 120) &
        (df["trip_distance"] > 0) &
        (df["trip_distance"] < 100) &
        (df["fare_amount"] > 0) &
        (df["passenger_count"] > 0)
    ].dropna()

    df = df.drop(columns=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    engine = create_engine(DB_URL)
    with engine.connect() as con:
        df.to_sql("taxi_trips", con, if_exists="append", index=False)

    print(f"Залито {len(df)} строк за {year}-{month:02d}")

def get_latest_available_month() -> tuple[int, int]:
    """Находит последний доступный месяц на сайте NYC."""
    now = datetime.now()
    for months_back in range(1, 6):
        date = now - relativedelta(months=months_back)
        url = TAXI_URL.format(year=date.year, month=date.month)
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            return date.year, date.month
    raise RuntimeError("Не удалось найти доступные данные")

if __name__ == "__main__":
    year, month = get_latest_available_month()
    print(f"Последние доступные данные: {year}-{month:02d}")
    download_and_load(year, month)
