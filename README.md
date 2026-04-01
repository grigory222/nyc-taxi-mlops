# NYC Taxi Duration Predictor

MLOps пет-проект: автоматизированный пайплайн сбора данных, обучения моделей и инференса на данных NYC Yellow Taxi.

## Описание

Проект предсказывает **длительность поездки на такси в Нью-Йорке** в минутах на основе параметров поездки: район отправления и прибытия, дистанция, тариф, время суток и день недели.

Данные — реальные записи о поездках жёлтого такси Нью-Йорка (NYC TLC Yellow Taxi), ~3 млн строк.

## Как идут данные
```
NYC TLC сайт (parquet файлы)
        ↓
fill_db.py — разовая начальная загрузка
        ↓
PostgreSQL — хранение всех поездок
        ↓
nyc_taxi_ingest DAG — ежемесячно проверяет новые данные,
                      заливает в PostgreSQL, триггерит обучение
        ↓
nyc_taxi_train DAG:
  get_data_from_postgres — читает из PostgreSQL, нормализует,
                           сохраняет prepared данные и scaler в S3
        ↓
  train_model x3 — параллельно обучают три модели
        ↓
  save_results — выбирает лучшую по R², сохраняет в S3
        ↓
Yandex Cloud S3:
  models/best_model.pkl   — лучшая модель
  models/scaler.pkl       — нормализатор для инференса
  models/*.pkl            — все три модели
  results/{дата}.json     — метрики каждого обучения
        ↓
FastAPI — загружает модель и scaler из S3, отдаёт предсказания
```

## Стек

- **Apache Airflow 3** — оркестрация пайплайнов
- **PostgreSQL** — хранение данных о поездках
- **Yandex Cloud Object Storage (S3)** — хранение моделей и метрик
- **scikit-learn** — обучение моделей
- **FastAPI** — REST API для инференса
- **Docker** — контейнеризация PostgreSQL и API

## DAG: nyc_taxi_train

![nyc_taxi_train DAG](docs/train_dag.png)

Запускается вручную или по триггеру от `nyc_taxi_ingest`.
```
init → get_data_from_postgres → [train_model x3] → save_results
```

**`init`** — логирует старт пайплайна.

**`get_data_from_postgres`** — читает 100к случайных строк из PostgreSQL, делает train/test split (80/20), нормализует через StandardScaler. Сохраняет в S3 четыре массива (`X_train`, `X_test`, `y_train`, `y_test`) и `scaler.pkl`.

**`train_model` x3** — три таска запускаются параллельно, каждый обучает свою модель: RandomForest, GradientBoosting, LinearRegression. Скачивают данные из S3, обучают модель, считают R²/RMSE/MAE, сохраняют модель в S3, возвращают метрики через XCom.

**`save_results`** — получает метрики всех моделей из XCom, выбирает лучшую по R², копирует её в `models/best_model.pkl`, сохраняет все метрики в `results/{дата}.json`.

## DAG: nyc_taxi_ingest

![nyc_taxi_ingest DAG](docs/ingest_dag.png)

Запускается автоматически 1-го числа каждого месяца.
```
check_new_data → check_already_loaded → download_and_load → should_retrain → [trigger_training | skip]
```

**`check_new_data`** — делает HEAD запрос к NYC TLC, ищет последний доступный месяц. Возвращает `{"year": ..., "month": ...}` или `None`.

**`check_already_loaded`** — проверяет в PostgreSQL не загружены ли уже данные за этот период. Защита от дублирования.

**`download_and_load`** — скачивает parquet, делает feature engineering, фильтрует выбросы, заливает в PostgreSQL.

**`should_retrain`** — ветвление: если данные залиты — триггерит `nyc_taxi_train`, иначе — `skip`.

## Быстрый старт

### 1. Клонировать репозиторий
```bash
git clone https://github.com/grigory222/nyc-taxi-mlops
cd nyc-taxi-mlops
```

### 2. Создать .env файл
```bash
cp .env.example .env
# заполнить своими значениями
```

### 3. Поднять PostgreSQL
```bash
docker compose up postgres -d
```

### 4. Залить начальные данные
```bash
conda activate nyc_taxi_env
python fill_db.py
```

Скрипт автоматически найдёт последний доступный месяц на сайте NYC TLC и загрузит его.

### 5. Запустить Airflow
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone
```

Открыть http://localhost:8080, добавить connections:

**pg_connection** (тип Postgres):
- Host: `localhost`
- Database: `airflow_db`
- Login: `airflow_user`
- Password: `airflow_pass`
- Port: `5432`

**s3_connection** (тип Amazon Web Services):
- AWS Access Key ID: ваш ключ
- AWS Secret Access Key: ваш секрет
- Extra: `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}`

Запустить DAG `nyc_taxi_train` вручную через Trigger.

### 6. Запустить API
```bash
docker compose up api -d
```

Или локально:
```bash
uvicorn api.main:app --reload
```

## API

### Предсказание длительности поездки
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "passenger_count": 2,
    "trip_distance": 3.5,
    "PULocationID": 161,
    "DOLocationID": 237,
    "fare_amount": 14.5,
    "pickup_hour": 18,
    "pickup_dayofweek": 2,
    "is_weekend": 0
  }'
```

Ответ:
```json
{"duration_min":12.24,"model":"random_forest"}
```

### Эндпоинты

- `POST /predict` — предсказание длительности поездки
- `GET /health` — статус сервиса и состояние модели
- `GET /model/info` — название модели и метрики последнего обучения
- `GET /docs` — Swagger UI

## Структура проекта
```
nyc-taxi-mlops/
├── api/
│   ├── main.py         # FastAPI роуты
│   ├── model.py        # загрузка модели из S3, инференс
│   ├── schemas.py      # Pydantic схемы запросов и ответов
│   └── config.py       # настройки из .env
├── airflow/
│   └── dags/
│       ├── train_dag.py   # DAG обучения моделей
│       └── ingest_dag.py  # DAG загрузки новых данных
├── docs/
│   ├── train_dag.png
│   └── ingest_dag.png
├── fill_db.py          # начальная загрузка данных
├── docker-compose.yml
├── Dockerfile
├── requirements-api.txt
├── .env.example
└── README.md
```