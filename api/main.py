import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

from api.model import model_service
from api.schemas import (
    TripFeatures,
    PredictionResponse,
    HealthResponse,
    ModelInfoResponse,
)

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    model_service.load()
    yield


app = FastAPI(
    title="NYC Taxi Duration Predictor",
    description="Predicts taxi trip duration in minutes",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        model_loaded=model_service.is_loaded,
    )


@app.get("/model/info", response_model=ModelInfoResponse)
def model_info() -> ModelInfoResponse:
    if not model_service.is_loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return ModelInfoResponse(**model_service.metrics)


@app.post("/predict", response_model=PredictionResponse)
def predict(trip: TripFeatures) -> PredictionResponse:
    if not model_service.is_loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")

    duration = model_service.predict(trip.model_dump())
    return PredictionResponse(
        duration_min=duration,
        model=model_service.model_name,
    )
