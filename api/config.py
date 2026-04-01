from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    bucket: str = "test2-bucket-adsadas"
    s3_endpoint: str = "https://storage.yandexcloud.net"
    s3_region: str = "ru-central1"
    aws_access_key_id: str
    aws_secret_access_key: str

    class Config:
        env_file = ".env"


settings = Settings()
