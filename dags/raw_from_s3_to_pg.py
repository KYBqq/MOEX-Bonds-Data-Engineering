import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "const"
DAG_ID = "raw_from_s3_to_pg"

# Используемые параметры
LAYER = "raw"
SOURCE = "moex_ofz"
SCHEMA = "ods"
TARGET_TABLE = "dwh_bond"

# S3 доступ
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# Postgres через DuckDB
PASSWORD = Variable.get("pg_password")

# Описания
LONG_DESCRIPTION = """
DAG загружает сырые данные по торгам облигациями из S3 в Postgres (DuckDB через HTTPFS).
"""
SHORT_DESCRIPTION = "Загрузка торгов облигациями"

# Аргументы по умолчанию
default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    start_date = context['data_interval_start'].format('YYYY-MM-DD')
    end_date = context['data_interval_end'].format('YYYY-MM-DD')
    return start_date, end_date


def get_and_transfer_raw_data_to_ods_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")

    con = duckdb.connect()
    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            boardid,
            tradedate,
            shortname,
            secid,
            numtrades,
            value,
            low,
            high,
            close,
            legalcloseprice,
            accint,
            waprice,
            yieldclose,
            open,
            volume,
            marketprice2,
            marketprice3,
            mp2valtrd,
            marketprice3tradesvalue,
            matdate,
            duration,
            yieldatwap,
            couponpercent,
            couponvalue,
            lasttradedate,
            facevalue,
            currencyid,
            faceunit,
            tradingsession,
            trade_session_date
        )
        SELECT
            boardId AS boardid,
            tradeDate::DATE AS tradedate,
            shortName AS shortname,
            secId AS secid,
            numTrades AS numtrades,
            value,
            low,
            high,
            close,
            legalClosePrice AS legalcloseprice,
            accInt AS accint,
            waiverPrice AS waprice,
            yieldClose AS yieldclose,
            openPrice AS open,
            volume,
            marketPrice2 AS marketprice2,
            marketPrice3 AS marketprice3,
            mp2ValTrd AS mp2valtrd,
            marketPrice3TradesValue AS marketprice3tradesvalue,
            matDate::DATE AS matdate,
            duration,
            yieldAtWaP AS yieldatwap,
            couponPercent AS couponpercent,
            couponValue AS couponvalue,
            lastTradeDate::DATE AS lasttradedate,
            faceValue AS facevalue,
            currencyId AS currencyid,
            faceUnit AS faceunit,
            tradingSession AS tradingsession,
            tradeSessionDate::DATE AS trade_session_date
        FROM 's3://dev/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
    """
    )
    con.close()
    logging.info(f"✅ Download for date success: {start_date}")

with DAG(
    dag_id=DAG_ID,
    schedule_interval='0 5 * * *',
    default_args=default_args,
    tags=['s3', 'ods', 'pg'],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id='start',
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id='sensor_on_raw_layer',
        external_dag_id='raw_from_api_to_s3',
        allowed_states=['success'],
        mode='reschedule',
        timeout=360000,
        poke_interval=60,
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id='get_and_transfer_raw_data_to_ods_pg',
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end
