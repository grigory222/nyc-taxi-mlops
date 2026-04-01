import pandas as pd
import requests
from sqlalchemy import create_engine
import os

DB_URL = "postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db"
TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

def download_and_load(year: int, month: int):
    url = TAXI_URL.format(year=year, month=month)
    print(f"Скачиваем {url}...")
    
    df = pd.read_parquet(url)
    
    # Оставляем нужные колонки
    df = df[["tpep_pickup_datetime", "tpep_dropoff_datetime",
             "passenger_count", "trip_distance",
             "PULocationID", "DOLocationID",
             "fare_amount"]].copy()
    
    # Feature engineering
    df["duration_min"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_dayofweek"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["is_weekend"] = df["pickup_dayofweek"].isin([5, 6]).astype(int)
    df["month"] = month
    df["year"] = year

    # Фильтрация выбросов
    df = df[(df["duration_min"] >= 1) & (df["duration_min"] <= 120)]
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] < 100)]
    df = df[df["fare_amount"] > 0]
    df = df.dropna()

    df = df.drop(columns=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    engine = create_engine(DB_URL)
    with engine.connect() as con:
        df.to_sql("taxi_trips", con, if_exists="append", index=False)
    
    print(f"Залито {len(df)} строк за {year}-{month:02d}")

if __name__ == "__main__":
    download_and_load(2024, 1)  # январь для старта
