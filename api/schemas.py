from pydantic import BaseModel, Field


class TripFeatures(BaseModel):
    passenger_count: float = Field(..., ge=1, le=8, example=2)
    trip_distance: float = Field(..., gt=0, example=3.5)
    PULocationID: int = Field(..., example=161)
    DOLocationID: int = Field(..., example=237)
    fare_amount: float = Field(..., gt=0, example=14.5)
    pickup_hour: int = Field(..., ge=0, le=23, example=18)
    pickup_dayofweek: int = Field(..., ge=0, le=6, example=2)
    is_weekend: int = Field(..., ge=0, le=1, example=0)


class PredictionResponse(BaseModel):
    duration_min: float
    model: str


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool


class ModelInfoResponse(BaseModel):
    model: str
    r2: float
    rmse: float
    mae: float
    trained_at: str
