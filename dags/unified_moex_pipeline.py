import logging
import requests
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "const"
DAG_ID = "unified_moex_pipeline"

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
SHORT_DESCRIPTION = "Единый пайплайн: загрузка OFZ с MOEX в S3 и затем в PostgreSQL"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    exec_date = context["execution_date"]
    prev_date = (exec_date - pendulum.duration(days=1)).format("YYYY-MM-DD")
    return prev_date, prev_date

def fetch_and_store_ofz(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"Start loading OFZ from {start_date} to {end_date}")

    # 1) Получаем список всех облигаций через правильный API endpoint
    url_list = (
        "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
        "?iss.meta=off"
    )
    
    try:
        resp = requests.get(url_list, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("securities")
        if not data:
            logging.error("Нет блока 'securities' в ответе")
            return
    except Exception as e:
        logging.error(f"Ошибка при получении списка бумаг: {e}")
        return

    sec_cols = data["columns"]
    sec_rows = data["data"]

    logging.info(f"Получено {len(sec_rows)} облигаций всего")

    # Найдем индекс колонки SECID
    secid_idx = sec_cols.index("SECID")

    # Получаем все облигации
    secids = []
    for row in sec_rows:
        if row and len(row) > secid_idx:
            secid = row[secid_idx] if row[secid_idx] else ""
            
            # Добавляем все облигации с непустым SECID
            if secid:
                secids.append(secid)

    logging.info(f"Найдено {len(secids)} облигаций для загрузки")

    # Ограничиваем до 100 инструментов
    secids = secids[:100]
    logging.info(f"Ограничено до {len(secids)} облигаций")

    # 2) Скачиваем историю по каждому secid
    all_rows = []
    cols = None
    successful_downloads = 0
    
    for sec in secids:
        url_hist = (
            f"https://iss.moex.com/iss/history/engines/stock/markets/bonds/"
            f"boards/TQOB/securities/{sec}.json"
            f"?from={start_date}&till={end_date}&iss.meta=off"
        )
        
        try:
            r = requests.get(url_hist, timeout=30)
            if not r.ok:
                logging.warning(f"Ошибка {r.status_code} при запросе {sec}")
                continue

            hist_data = r.json().get("history", {})
            cols_raw = hist_data.get("columns", [])
            if not cols:
                cols = cols_raw[:]  # Копируем оригинальные названия колонок
            
            rows_count = 0
            for row in hist_data.get("data", []):
                if row:
                    # Проверяем, есть ли уже колонка SECID в данных
                    if "SECID" in cols:
                        # Если SECID уже есть в данных, используем её
                        all_rows.append(row)
                    else:
                        # Если SECID нет, добавляем значение sec в конец строки
                        all_rows.append(row + [sec])
                    rows_count += 1
            
            if rows_count > 0:
                successful_downloads += 1
                
        except Exception as e:
            logging.warning(f"Ошибка при загрузке данных для {sec}: {e}")
            continue

    if not all_rows:
        logging.warning(f"Нет исторических данных за период {start_date} - {end_date}")
        logging.info(f"Успешно обработано {successful_downloads} из {len(secids)} инструментов")
        return

    # Добавляем колонку SECID только если её ещё нет
    if "SECID" not in cols:
        cols.append("SECID")
        logging.info(f"Добавлена колонка SECID")
    else:
        logging.info(f"Колонка SECID уже существует в API response")
    
    logging.info(f"Итого загружено {len(all_rows)} записей")

    # 3) Сохраняем в S3 через DuckDB
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;
        """)

        # Создаем временную таблицу из данных
        col_definitions = []
        for i, col in enumerate(cols):
            # Определяем тип данных на основе первой строки
            sample_val = all_rows[0][i] if all_rows and len(all_rows[0]) > i else None
            if isinstance(sample_val, (int, float)) and sample_val is not None:
                col_type = "DOUBLE" if isinstance(sample_val, float) else "BIGINT"
            else:
                col_type = "VARCHAR"
            col_definitions.append(f'"{col}" {col_type}')
        
        # Создаем таблицу
        create_table_sql = f"""
        CREATE TABLE ofz_temp ({', '.join(col_definitions)})
        """
        con.execute(create_table_sql)
        
        # Вставляем данные порциями
        batch_size = 1000
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]
            
            # Подготавливаем VALUES для вставки
            values_list = []
            for row in batch:
                # Экранируем строковые значения и обрабатываем NULL
                escaped_row = []
                for val in row:
                    if val is None:
                        escaped_row.append("NULL")
                    elif isinstance(val, str):
                        # Экранируем одинарные кавычки
                        escaped_val = val.replace("'", "''")
                        escaped_row.append(f"'{escaped_val}'")
                    else:
                        escaped_row.append(str(val))
                values_list.append(f"({', '.join(escaped_row)})")
            
            values_str = ', '.join(values_list)
            insert_sql = f"INSERT INTO ofz_temp VALUES {values_str}"
            con.execute(insert_sql)

        # Копируем в Parquet на S3
        s3_path = f"s3://dev/{LAYER}/{SOURCE}/{start_date}/{start_date}_ofz.parquet"
        con.execute(f"""
            COPY (SELECT * FROM ofz_temp)
            TO '{s3_path}'
            (FORMAT 'parquet', COMPRESSION 'gzip');
        """)
        
        # Очищаем временную таблицу
        con.execute("DROP TABLE ofz_temp")
        con.close()
        logging.info(f"✅ Bond data saved to {s3_path}")
        
    except Exception as e:
        logging.error(f"Ошибка при сохранении в S3: {e}")
        if 'con' in locals():
            try:
                con.execute("DROP TABLE IF EXISTS ofz_temp")
                con.close()
            except:
                pass
        raise

def transfer_s3_to_pg(**context):
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
            '{start_date}'::DATE AS trade_session_date
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
    schedule_interval='0 7 * * *',  # Запуск в 7:00
    default_args=default_args,
    tags=['moex', 'unified', 'pipeline'],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start')

    # Первый этап: загрузка данных с MOEX в S3
    fetch_and_store_ofz_task = PythonOperator(
        task_id='fetch_and_store_ofz',
        python_callable=fetch_and_store_ofz,
    )

    # Второй этап: перенос данных из S3 в PostgreSQL
    transfer_s3_to_pg_task = PythonOperator(
        task_id='transfer_s3_to_pg',
        python_callable=transfer_s3_to_pg,
    )

    end = EmptyOperator(task_id='end')

    # Цепочка выполнения
    start >> fetch_and_store_ofz_task >> transfer_s3_to_pg_task >> end

