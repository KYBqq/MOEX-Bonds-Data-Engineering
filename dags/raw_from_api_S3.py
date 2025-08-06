import logging
import requests
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "const"
DAG_ID = "raw_ofz_from_moex_to_s3"

LAYER = "raw"
SOURCE = "moex_ofz"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

SHORT_DESCRIPTION = "Загрузка исторических котировок всех Облигаций с MOEX ISS API в S3"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    start = context["data_interval_start"].format("YYYY-MM-DD")
    end = context["data_interval_end"].format("YYYY-MM-DD")
    return start, end

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
                cols = cols_raw  # Оставляем оригинальные названия колонок
            
            rows_count = 0
            for row in hist_data.get("data", []):
                if row:
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

    # Добавляем колонку SECID
    cols.append("SECID")
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

        # Регистрируем данные
        con.register("ofz_data", (all_rows, cols))

        # Копируем в Parquet на S3
        s3_path = f"s3://dev/{LAYER}/{SOURCE}/{start_date}/{start_date}_ofz.parquet"
        con.execute(f"""
            COPY (SELECT * FROM ofz_data)
            TO '{s3_path}'
            (FORMAT 'parquet', COMPRESSION 'gzip');
        """)
        con.close()
        logging.info(f"✅ OFZ data saved to {s3_path}")
        
    except Exception as e:
        logging.error(f"Ошибка при сохранении в S3: {e}")
        if 'con' in locals():
            con.close()
        raise

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 6 * * *",
    tags=["moex", "raw", "ofz"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    load_ofz = PythonOperator(
        task_id="fetch_and_store_ofz",
        python_callable=fetch_and_store_ofz,
    )

    end = EmptyOperator(task_id="end")

    start >> load_ofz >> end