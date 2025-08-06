import logging

import requests
import pandas as pd
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "i.korsakov"
DAG_ID = "raw_ofzpd_from_moex_to_s3"

LAYER = "raw"
SOURCE = "moex_ofzpd"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

SHORT_DESCRIPTION = "Загрузка исторических котировок ОФЗ-ПД с MOEX ISS API в S3"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2024, 1, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    start = context["data_interval_start"].format("YYYY-MM-DD")
    end   = context["data_interval_end"].format("YYYY-MM-DD")
    return start, end

def fetch_and_store_ofzpd(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"Start loading OFZ-PD from {start_date} to {end_date}")

    # 1) Получаем список ВСЕХ ОФЗ-ПД
    url_list = "https://iss.moex.com/iss/securities.json?q=ОФЗ-ПД&iss.meta=off"
    resp = requests.get(url_list)
    resp.raise_for_status()
    sec_data = resp.json()["securities"]
    sec_cols = sec_data["columns"]
    sec_rows = sec_data["data"]
    secids = [row[sec_cols.index("SECID")] for row in sec_rows]

    if not secids:
        logging.warning("No OFZ-PD found, exiting")
        return

    # 2) Собираем историю по каждому SECID
    all_rows = []
    for sec in secids:
        url_hist = (
            "https://iss.moex.com/iss/history"
            f"/engines/stock/markets/bonds/boards/TQOB"
            f"/securities/{sec}.json"
            f"?from={start_date}&till={end_date}&iss.meta=off"
        )
        r = requests.get(url_hist)
        if not r.ok:
            logging.warning(f"Failed to fetch {sec}: HTTP {r.status_code}")
            continue
        hist = r.json().get("history", {})
        cols = hist.get("columns", [])
        for row in hist.get("data", []):
            all_rows.append(row + [sec])
    cols.append("SECID")

    if not all_rows:
        logging.warning("No historical data loaded, exiting")
        return

    # 3) Создаём pandas DataFrame и регистрируем в DuckDB
    df = pd.DataFrame(all_rows, columns=cols)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
    """)
    con.register("ofzpd_df", df)

    # 4) Выгружаем в Parquet на S3
    s3_path = f"s3://dev/{LAYER}/{SOURCE}/{start_date}/{start_date}_ofzpd.parquet"
    con.execute(f"""
        COPY ofzpd_df
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
