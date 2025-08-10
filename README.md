## MOEX Bonds Data Engineering

![Untitled diagram _ Mermaid Chart-2025-08-08-115113](https://github.com/user-attachments/assets/bd8439b7-d24c-4299-9a74-613b423191f5)

Проект по сбору и загрузке исторических данных по облигациям, биржа MOEX.

Технологический стек:

```
Инфраструктура — Docker Compose;
Оркестрация — Apache Airflow; 
Файловое хранилище — S3 (MinIO);
DWH — PostgreSQL; 
ETL/перенос — DuckDB; 
Визуализация — Apache Superset.
```

Airflow:

![ezgif-56cac16664d0a9](https://github.com/user-attachments/assets/fb723e63-18d1-485f-956e-96b083c2d46e)

S3 (MinIO):

![ezgif-37690438834369](https://github.com/user-attachments/assets/7ed14ef1-5ed8-4d4b-84da-a6f3cbb6f6e5)

Superset:

![ezgif-256a275535e78a](https://github.com/user-attachments/assets/7a8de85b-f8b6-4a9a-8871-ee2810ef3267)

### Состав и стек
- Docker Compose-сервис: `superset`, `postgres_dwh` (PostgreSQL), `minio`, `redis`, `airflow-*` (webserver, scheduler, worker, triggerer)
- ETL/оркестрация: Apache Airflow (DAG `unified_moex_pipeline`)
- Промежуточное хранилище файлов: MinIO (S3 API)
- DWH: PostgreSQL (схемы `ods`, `dm`)
- Витрины данных: `dm.dim_bond`, `dm.fct_bond_day`

### Структура репозитория (важное)
- `dags/unified_moex_pipeline.py` — единый пайплайн: загрузка с MOEX → S3 (MinIO) → Postgres (`ods.dwh_bond`) → триггер витрин
- `dags/fct_count_day_bond.py` — расчёт витрин `dm.dim_bond` и `dm.fct_bond_day`
- `docker-compose.yaml` — сервисы проекта
- `requirements.txt` — зависимости для локальной разработки (Airflow, DuckDB)

### Порты и доступы по умолчанию
- Superset: `http://localhost:8088` (первый вход — создайте пользователя через CLI, см. ниже)
- Airflow: `http://localhost:8080` (логин/пароль: `airflow`/`airflow`)
- MinIO: `http://localhost:9001` (консоль), API на `:9000` (логин/пароль: `minioadmin`/`minioadmin`)
- Postgres DWH: `localhost:5432` (внутри docker-сети — хост `postgres_dwh`; логин/пароль/БД: `postgres`/`postgres`/`postgres`)

---

## Быстрый старт

1) Запуск инфраструктуры

```bash
docker compose up -d
# подождите 10 секунд на прогрев сервисов
```

2) Инициализация Superset (метаданные и админ)

В `docker-compose.yaml` уже задано:
- `SUPERSET_DATABASE_URI=postgresql+psycopg2://postgres:postgres@postgres_dwh:5432/postgres`
- `PIP_ADDITIONAL_REQUIREMENTS=psycopg2-binary==2.9.9` (чтобы Superset видел драйвер Postgres)

Выполните команды один раз:

```bash
docker compose exec superset superset db upgrade
docker compose exec superset superset init

# создать администратора (пример)
docker compose exec superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@local --password 'admin'

docker compose restart superset
```

3) Переменные Airflow

В DAG используются переменные Airflow (UI: Admin → Variables):
- `access_key` — ключ MinIO (S3)
- `secret_key` — секрет MinIO (S3)
- `pg_password` — пароль пользователя `postgres` (используется DuckDB Postgres extension)

4) Подключение DWH в Superset

В Superset → Data → Databases → + Database → PostgreSQL укажите SQLAlchemy URI:

```
postgresql+psycopg2://postgres:postgres@postgres_dwh:5432/postgres?sslmode=disable
```

Примечание: подключение выполняется через UI Superset. Если тест коннекта падает, сначала убедитесь, что создан администратор (см. п.2) и установлен драйвер `psycopg2` (см. docker-compose).

5) Создание датасетов в Superset

- Data → Datasets → + Dataset
- Выберите вашу БД → схема `dm` → таблицы `dm.dim_bond`, `dm.fct_bond_day`

6) Запуск пайплайна в Airflow

- Откройте Airflow → DAGs
- Запустите `unified_moex_pipeline`
  - Шаг 1: загрузка исторических данных по облигациям с MOEX в MinIO (S3)
  - Шаг 2: перенос в PostgreSQL `ods.dwh_bond`
  - Шаг 3: триггер витрин (`fct_count_day_bond`) — наполнение `dm.dim_bond` и `dm.fct_bond_day`

---

## Архитектура данных

- Источник: MOEX ISS API (список облигаций + история котировок)
- S3 (MinIO): хранение Parquet-файлов выгрузок по датам
- DWH (PostgreSQL):
  - `ods.dwh_bond` — сырые/приведённые данные по облигациям за торговую дату
  - `dm.dim_bond` — размерность облигаций (уникальные `secid` и атрибуты)
  - `dm.fct_bond_day` — факт по дням: агрегаты (объём, сделки, мин/макс/средние)

### Почему используем S3 (MinIO)

- Что такое S3: объектное хранилище, доступное по HTTP(S). Данные хранятся как объекты в «бакетах», к ним обращаются по ключу/пути. Масштабируемо, дешево, отказоустойчиво. MinIO — self-hosted реализация S3 API, полностью совместимая с AWS S3.
- Зачем в этом проекте:
  - Буфер между парсингом и загрузкой в DWH: надёжный staging-слой. Если БД недоступна, данные не теряются.
  - Идемпотентность и воспроизводимость: складываем «сырые» выгрузки по датам; легко перегрузить/переиграть этап загрузки в БД без повторного парсинга.
  - Разделение нагрузок: чтение/агрегации выполняем из файлов (Parquet), не нагружая Postgres лишними операциями.
  - Эффективное хранение: Parquet + gzip — колонночный формат, сжатие и быстрые сканы для аналитики.
  - Совместимость и переносимость: те же файлы читают DuckDB, Spark, Pandas и др.; локально MinIO, в облаке — AWS S3 без переписывания кода.
  - Параллелизация и версионирование: каталогизация путём `s3://dev/raw/moex_ofz/<YYYY-MM-DD>/<date>_ofz.parquet` позволяет легко распараллеливать и отслеживать версии.
- Как мы работаем с S3 в коде:
  - В DuckDB: `INSTALL httpfs; LOAD httpfs;` и настройки `s3_*` (endpoint `minio:9000`, `s3_url_style='path'`, без SSL локально).
  - Доступы берём из Airflow Variables: `access_key`, `secret_key`. Секреты не храним в репозитории.

---

## Траблшутинг

- «Could not load database driver: PostgresEngineSpec» / `ModuleNotFoundError: psycopg2`
  - В контейнере Superset должен быть установлен драйвер. В `docker-compose.yaml` уже добавлено `PIP_ADDITIONAL_REQUIREMENTS=psycopg2-binary==2.9.9`.
  - Проверьте импорт из контейнера:
    ```bash
    docker compose exec superset python -c "import psycopg2; print(psycopg2.__version__)"
    ```

- 500 Internal Server Error в Superset
  - Частая причина — не создан администратор. Создайте его: 
    `docker compose exec superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@local --password 'admin'` и перезапустите Superset.
  - Также выполните инициализацию метаданных: `superset db upgrade` и `superset init`, затем `docker compose restart superset` и подождите ~10 секунд.

- «No PIL installation found» в логах Superset
  - Это предупреждение о неустановленном Pillow; влияет только на скриншоты/PDF отчётов. На подключение к БД не влияет.

- «No such container: postgres_dwh»
  - Используйте команды через `docker compose` (а не `docker exec`): `docker compose up -d postgres_dwh`, `docker compose exec postgres_dwh psql ...`

- Быстрые проверки
  ```bash
  docker compose ps
  docker compose logs --tail=200 superset
  docker compose exec superset python -c "import psycopg2; psycopg2.connect(host='postgres_dwh', dbname='postgres', user='postgres', password='postgres').close(); print('OK')"
  ```

---

## Примечания

- В коде DAG используется DuckDB (`INSTALL httpfs`, `LOAD postgres`) для простого ETL.
- Переменные окружения (ключи S3 и пароль Postgres) подтягиваются из переменных Airflow (`Variable.get(...)`).

#### Как сделано у нас
- В текущих DAG `conn_id` для Postgres не используется.
- Подключение к DWH выполняется через DuckDB Postgres extension:
  - HOST `postgres_dwh`, DB `postgres`, USER `postgres`, PASSWORD из Airflow Variable `pg_password`.
- Доступ к S3 для DuckDB (`httpfs`) берётся из Airflow Variables: `access_key`, `secret_key`.
- Для прод-сред — хранить секреты в Airflow Connections/Secrets Backend.

### Airflow: conn_id через Variable.get — чем чревато

- Нет переменной — DAG не парсится (поможет `default_var`).
- Секреты в Variables хранить нельзя (пароли/URI — только Connections/Secrets).
- Смена переменной меняет коннект со следующего парсинга.
- По коду неочевидно, какой коннект — сложнее поддержка.

Рекомендуем: фиксировать `conn_id` в коде (например, `postgres_dwh`), секреты — в Connections/Secrets.

### Чек-лист: если после создания учётки Superset всё ещё 500

1) Убедитесь, что создан администратор (после первого запуска это обязательно):
   ```bash
   docker compose exec superset superset fab create-admin \
     --username admin --firstname Admin --lastname User \
     --email admin@local --password 'admin'
   ```

2) Инициализируйте метаданные Superset и перезапустите сервис:
   ```bash
   docker compose exec superset superset db upgrade
   docker compose exec superset superset init
   docker compose restart superset
   # подождите ~10 секунд
   ```

3) Проверьте, что установлен драйвер Postgres в контейнере Superset:
   - В compose уже добавлено: `PIP_ADDITIONAL_REQUIREMENTS=psycopg2-binary==2.9.9`
   - Быстрая проверка/установка вручную (при необходимости):
     ```bash
     docker compose exec superset python -c "import psycopg2; print(psycopg2.__version__)" || \
     docker compose exec superset pip install --no-cache-dir psycopg2-binary==2.9.9
     docker compose restart superset
     ```

4) Протестируйте коннект к DWH из контейнера и только потом добавляйте подключение в UI:
   ```bash
   docker compose exec superset python -c "import psycopg2; psycopg2.connect(host='postgres_dwh', dbname='postgres', user='postgres', password='postgres').close(); print('OK')"
   ```

---

## Требования к среде (локально)

- Минимум для запуска (Docker Desktop):
  - CPU: 4 vCPU
  - RAM: 8 GB
  - Диск: 150+ GB свободного места
  - Порты: 8080 (Airflow), 8088 (Superset), 9000/9001 (MinIO), 5432 (Postgres)
- Рекомендуемо для комфортной работы на ПК/локальном сервере:
  - CPU: 4–8 vCPU
  - RAM: 16 GB
  - Диск: 200+ GB (с запасом для Parquet и логов)
  - Сеть: стабильное подключение для работы c MOEX API

 


