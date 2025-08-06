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

    try:
        con = duckdb.connect()
        
        # Настройка S3 и PostgreSQL
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL postgres; LOAD postgres;")
        
        con.execute(f"""
            SET TIMEZONE='UTC';
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;
        """)

        # Проверяем существование файла в S3
        s3_path = f"s3://dev/{LAYER}/{SOURCE}/{start_date}/{start_date}_ofz.parquet"
        
        try:
            result = con.execute(f"SELECT COUNT(*) FROM '{s3_path}'").fetchone()
            row_count = result[0] if result else 0
            logging.info(f"Found {row_count} rows in {s3_path}")
            
            if row_count == 0:
                logging.warning(f"No data found in {s3_path}")
                return
                
        except Exception as e:
            logging.error(f"Cannot read file {s3_path}: {e}")
            raise

        # Создание секрета для PostgreSQL
        con.execute(f"""
            CREATE SECRET IF NOT EXISTS dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh',
                PORT 5432,
                DATABASE 'postgres',
                USER 'postgres',
                PASSWORD '{PASSWORD}'
            );
        """)

        # Подключение к PostgreSQL
        con.execute("ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);")

        # Проверяем существование целевой таблицы
        try:
            con.execute(f"DESCRIBE dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}")
            logging.info(f"Target table {SCHEMA}.{TARGET_TABLE} exists")
        except Exception as e:
            logging.error(f"Target table {SCHEMA}.{TARGET_TABLE} does not exist: {e}")
            raise

        # Читаем данные из S3 и вставляем в PostgreSQL
        insert_sql = f"""
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
            COALESCE(BOARDID, '') AS boardid,
            TRADEDATE::DATE AS tradedate,
            COALESCE(SHORTNAME, '') AS shortname,
            COALESCE(SECID, '') AS secid,
            COALESCE(NUMTRADES, 0) AS numtrades,
            COALESCE(VALUE, 0) AS value,
            COALESCE(LOW, 0) AS low,
            COALESCE(HIGH, 0) AS high,
            COALESCE(CLOSE, 0) AS close,
            COALESCE(LEGALCLOSEPRICE, 0) AS legalcloseprice,
            COALESCE(ACCINT, 0) AS accint,
            COALESCE(WAPRICE, 0) AS waprice,
            COALESCE(YIELDCLOSE, 0) AS yieldclose,
            COALESCE(OPEN, 0) AS open,
            COALESCE(VOLUME, 0) AS volume,
            COALESCE(MARKETPRICE2, 0) AS marketprice2,
            COALESCE(MARKETPRICE3, 0) AS marketprice3,
            COALESCE(MP2VALTRD, 0) AS mp2valtrd,
            COALESCE(MARKETPRICE3TRADESVALUE, 0) AS marketprice3tradesvalue,
            MATDATE::DATE AS matdate,
            COALESCE(DURATION, 0) AS duration,
            COALESCE(YIELDATWAP, 0) AS yieldatwap,
            COALESCE(COUPONPERCENT, 0) AS couponpercent,
            COALESCE(COUPONVALUE, 0) AS couponvalue,
            LASTTRADEDATE::DATE AS lasttradedate,
            COALESCE(FACEVALUE, 0) AS facevalue,
            COALESCE(CURRENCYID, '') AS currencyid,
            COALESCE(FACEUNIT, '') AS faceunit,
            COALESCE(TRADINGSESSION, '') AS tradingsession,
            COALESCE(TRADEDATE, CURRENT_DATE)::DATE AS trade_session_date
        FROM '{s3_path}'
        WHERE TRADEDATE IS NOT NULL
        """
        
        logging.info("Starting data insertion...")
        con.execute(insert_sql)
        
        # Проверяем количество вставленных записей
        inserted_count = con.execute(f"""
            SELECT COUNT(*) FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} 
            WHERE trade_session_date = '{start_date}'::DATE
        """).fetchone()[0]
        
        logging.info(f"✅ Successfully inserted {inserted_count} records for date: {start_date}")
        
    except Exception as e:
        logging.error(f"❌ Error in data transfer: {e}")
        raise
    finally:
        if 'con' in locals():
            con.close()


with DAG(
    dag_id=DAG_ID,
    schedule_interval='0 7 * * *',  # Запуск в 7:00, после первого DAG (6:00)
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

    # Упрощенная функция для External Task Sensor
    def get_external_execution_date(execution_date, **context):
        # Возвращаем ту же дату выполнения, что и у текущего DAG'а
        return execution_date

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id='sensor_on_raw_layer',
        external_dag_id='raw_ofz_from_moex_to_s3',
        external_task_id='end',
        execution_date_fn=get_external_execution_date,
        allowed_states=['success'],
        mode='poke',  # Изменено с 'reschedule' на 'poke'
        timeout=7200,  # 2 часа
        poke_interval=60,  # Проверяем каждую минуту
    )

    transfer_task = PythonOperator(
        task_id='get_and_transfer_raw_data_to_ods_pg',
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> sensor_on_raw_layer >> transfer_task >> end