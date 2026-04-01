import io
import json
import logging
import pickle

import boto3
import numpy as np

from api.config import settings

_LOG = logging.getLogger(__name__)

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


class ModelService:
    def __init__(self):
        self.model = None
        self.scaler = None
        self.model_name: str | None = None
        self.metrics: dict | None = None
        self._s3 = None

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = boto3.client(
                "s3",
                endpoint_url=settings.s3_endpoint,
                region_name=settings.s3_region,
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
            )
        return self._s3

    def _load_pickle(self, key: str):
        obj = self.s3.get_object(Bucket=settings.bucket, Key=key)
        return pickle.loads(obj["Body"].read())

    def load(self) -> None:
        _LOG.info("Loading model and scaler from S3...")
        self.model = self._load_pickle("models/best_model.pkl")
        self.scaler = self._load_pickle("models/scaler.pkl")

        # Загружаем последние метрики
        response = self.s3.list_objects_v2(
            Bucket=settings.bucket, Prefix="results/"
        )
        if "Contents" in response:
            latest = sorted(
                response["Contents"], key=lambda x: x["LastModified"]
            )[-1]
            result_obj = self.s3.get_object(
                Bucket=settings.bucket, Key=latest["Key"]
            )
            result = json.loads(result_obj["Body"].read())
            self.model_name = result["best_model"]["model"]
            self.metrics = result["best_model"]

        _LOG.info(f"Model loaded: {self.model_name}")

    def predict(self, features: dict) -> float:
        X = np.array([[features[f] for f in FEATURES]])
        X_scaled = self.scaler.transform(X)
        return round(float(self.model.predict(X_scaled)[0]), 2)

    @property
    def is_loaded(self) -> bool:
        return self.model is not None


model_service = ModelService()
