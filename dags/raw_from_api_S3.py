import logging
import requests
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "const"
DAG_ID = "raw_ofzpd_from_moex_to_s3"

LAYER = "raw"
SOURCE = "moex_ofzpd"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

SHORT_DESCRIPTION = "Загрузка исторических котировок ОФЗ-ПД с MOEX ISS API в S3"

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

def fetch_and_store_ofzpd(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"Start loading OFZ-PD from {start_date} to {end_date}")

    # 1) Получаем список ОФЗ-ПД c доски TQOB
    url_list = (
        "https://iss.moex.com/iss/engines/stock/markets/bonds/"
        "boards/TQOB/securities.json?iss.meta=off"
    )
    resp = requests.get(url_list)
    resp.raise_for_status()
    data = resp.json().get("securities")
    if not data:
        logging.error("Нет блока 'securities' в ответе")
        return

    sec_cols = data["columns"]      # ['secid', 'shortname', ...]
    sec_rows = data["data"]

    # Проверяем, что названия колонок совпадают
    if "secid" not in sec_cols or "shortname" not in sec_cols:
        logging.error(f"Ожидались 'secid' и 'shortname' в {sec_cols}")
        return

    secid_idx = sec_cols.index("secid")
    name_idx  = sec_cols.index("shortname")

    # Фильтруем только ОФЗ-ПД
    secids = [
        row[secid_idx]
        for row in sec_rows
        if row[name_idx] and "ОФЗ-ПД" in row[name_idx]
    ]
    if not secids:
        logging.warning("Не найдено ни одной ОФЗ-ПД")
        return

    logging.info(f"Найдено {len(secids)} выпусков ОФЗ-ПД")

    # 2) Скачиваем историю по каждому secid
    all_rows = []
    cols = None
    for sec in secids:
        url_hist = (
            f"https://iss.moex.com/iss/history/engines/stock/markets/bonds/"
            f"boards/TQOB/securities/{sec}.json"
            f"?from={start_date}&till={end_date}&iss.meta=off"
        )
        r = requests.get(url_hist)
        if not r.ok:
            logging.warning(f"Ошибка {r.status_code} при запросе {sec}")
            continue

        hist = r.json().get("history", {})
        cols = hist.get("columns", [])
        for row in hist.get("data", []):
            all_rows.append(row + [sec])

    if not all_rows:
        logging.warning("Нет исторических данных за период")
        return

    # Добавляем колонку secid
    cols.append("secid")

    # 3) Загружаем в DuckDB и пишем в S3
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
    con.register("ofzpd_data", (all_rows, cols))

    # Копируем в Parquet на S3
    s3_path = f"s3://dev/{LAYER}/{SOURCE}/{start_date}/{start_date}_ofzpd.parquet"
    con.execute(f"""
        COPY (SELECT * FROM ofzpd_data)
        TO '{s3_path}'
        (FORMAT 'parquet', COMPRESSION 'gzip');
    """)
    con.close()
    logging.info(f"✅ OFZ-PD data saved to {s3_path}")

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 6 * * *",
    tags=["moex", "raw", "ofzpd"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    load_ofzpd = PythonOperator(
        task_id="fetch_and_store_ofzpd",
        python_callable=fetch_and_store_ofzpd,
    )

    end = EmptyOperator(task_id="end")

    start >> load_ofzpd >> end
