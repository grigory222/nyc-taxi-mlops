import json
import logging
import pickle

import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

_LOG = logging.getLogger(__name__)

BUCKET = "test2-bucket-adsadas"
S3_ENDPOINT = "https://storage.yandexcloud.net"

FEATURES = [
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "pickup_hour",
    "pickup_dayofweek",
    "is_weekend",
]
TARGET = "duration_min"

MODELS = {
    "random_forest": RandomForestRegressor(n_estimators=100, random_state=42),
    "gradient_boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
    "linear_regression": LinearRegression(),
}


@dag(
    dag_id="nyc_taxi_train",
    schedule=None,  # запускается только вручную или по триггеру от ingest DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlops", "nyc_taxi"],
    default_args={
        "owner": "Grigory Voronov",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
)
def nyc_taxi_train():

    @task
    def init() -> None:
        _LOG.info("NYC Taxi train pipeline started.")

    @task
    def get_data_from_postgres() -> None:
        pg_hook = PostgresHook("pg_connection")
        con = pg_hook.get_conn()

        data = pd.read_sql_query(
            "SELECT * FROM taxi_trips ORDER BY RANDOM() LIMIT 100000", con
        )
        _LOG.info(f"Loaded {len(data)} rows from PostgreSQL.")

        X = data[FEATURES]
        y = data[TARGET]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        s3_hook = S3Hook("s3_connection")
        session = s3_hook.get_session("ru-central1")
        resource = session.resource("s3", endpoint_url=S3_ENDPOINT)

        resource.Object(BUCKET, "models/scaler.pkl").put(
            Body=pickle.dumps(scaler)
        )

        for name, array in zip(
            ["X_train", "X_test", "y_train", "y_test"],
            [X_train_scaled, X_test_scaled, y_train.values, y_test.values],
        ):
            resource.Object(BUCKET, f"datasets/prepared/{name}.pkl").put(
                Body=pickle.dumps(array)
            )

        _LOG.info("Data prepared and uploaded to S3.")
    
    
    @task
    def train_model(model_name: str) -> dict:
        s3_hook = S3Hook("s3_connection")
        session = s3_hook.get_session("ru-central1")
        resource = session.resource("s3", endpoint_url=S3_ENDPOINT)

        data = {}
        for name in ["X_train", "X_test", "y_train", "y_test"]:
            file = s3_hook.download_file(
                key=f"datasets/prepared/{name}.pkl", bucket_name=BUCKET
            )
            data[name] = pd.read_pickle(file)

        model = MODELS[model_name]
        model.fit(data["X_train"], data["y_train"])
        prediction = model.predict(data["X_test"])

        metrics = {
            "model": model_name,
            "r2": round(r2_score(data["y_test"], prediction), 4),
            "rmse": round(mean_squared_error(data["y_test"], prediction) ** 0.5, 4),
            "mae": round(mean_absolute_error(data["y_test"], prediction), 4),
            "trained_at": datetime.now().isoformat(),
        }

        # Сохраняем модель
        resource.Object(BUCKET, f"models/{model_name}.pkl").put(
            Body=pickle.dumps(model)
        )

        _LOG.info(f"{model_name} metrics: {metrics}")
        return metrics  # автоматически уходит в XCom

    @task
    def save_results(all_metrics: list[dict]) -> None:
        # Выбираем лучшую модель по R2
        best = max(all_metrics, key=lambda x: x["r2"])
        _LOG.info(f"Best model: {best['model']} with R2={best['r2']}")

        s3_hook = S3Hook("s3_connection")
        session = s3_hook.get_session("ru-central1")
        resource = session.resource("s3", endpoint_url=S3_ENDPOINT)

        # Копируем лучшую модель как best_model.pkl
        best_model_obj = resource.Object(
            BUCKET, f"models/{best['model']}.pkl"
        ).get()
        resource.Object(BUCKET, "models/best_model.pkl").put(
            Body=best_model_obj["Body"].read()
        )

        # Сохраняем все метрики в S3
        date = datetime.now().strftime("%Y_%m_%d_%H")
        result = {"best_model": best, "all_models": all_metrics}
        resource.Object(BUCKET, f"results/{date}.json").put(
            Body=json.dumps(result, ensure_ascii=False)
        )

        _LOG.info("Results saved.")

    init_task = init()
    get_data_task = get_data_from_postgres()
    metrics = [train_model(model_name) for model_name in MODELS.keys()]
    save_task = save_results(metrics)

    init_task >> get_data_task >> metrics >> save_task


nyc_taxi_train()
