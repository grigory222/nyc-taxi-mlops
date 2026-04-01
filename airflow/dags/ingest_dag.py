import logging
import requests
import pandas as pd

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text

from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

_LOG = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db"
TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"


@dag(
    dag_id="nyc_taxi_ingest",
    schedule="0 0 1 * *",  # раз в месяц 1-го числа
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mlops", "nyc_taxi"],
    default_args={
        "owner": "Grigory Voronov",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)
def nyc_taxi_ingest():

    @task
    def check_new_data() -> dict | None:
        now = datetime.now()
        year, month = now.year, now.month

        url = TAXI_URL.format(year=year, month=month)
        _LOG.info(f"Checking {url}...")

        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            _LOG.info(f"Data available: {year}-{month:02d}")
            return {"year": year, "month": month}

        _LOG.info(f"No data for {year}-{month:02d}")
        return None

    @task
    def check_already_loaded(period: dict | None) -> dict | None:
        if period is None:
            return None

        engine = create_engine(DB_URL)
        with engine.connect() as con:
            result = con.execute(
                text("SELECT COUNT(*) FROM taxi_trips WHERE year=:year AND month=:month"),
                {"year": period["year"], "month": period["month"]}
            ).fetchone()

        if result[0] > 0:
            _LOG.info(f"Already loaded: {period['year']}-{period['month']:02d}")
            return None

        return period

    @task
    def download_and_load(period: dict | None) -> bool:
        if period is None:
            _LOG.info("Nothing to load.")
            return False

        year, month = period["year"], period["month"]
        url = TAXI_URL.format(year=year, month=month)
        _LOG.info(f"Downloading {url}...")

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

        _LOG.info(f"Loaded {len(df)} rows for {year}-{month:02d}")
        return True

    @task.branch
    def should_retrain(loaded: bool) -> str:
        return "trigger_training" if loaded else "skip"

    @task
    def skip() -> None:
        _LOG.info("No new data — skipping retraining.")

    trigger_training = TriggerDagRunOperator(
        task_id="trigger_training",
        trigger_dag_id="nyc_taxi_train",
        wait_for_completion=False,
    )

    period = check_new_data()
    period = check_already_loaded(period)
    loaded = download_and_load(period)
    branch = should_retrain(loaded)
    branch >> [trigger_training, skip()]


nyc_taxi_ingest()
