import pendulum
import duckdb
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

# Конфигурация DAG
OWNER = "const"
DAG_ID = "fct_count_day_bond"

# Используемые таблицы в DAG
SCHEMA = "dm"
TARGET_TABLE = "fct_count_day_bond"

# DWH
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 8, 7, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,  # Отключаем расписание - DAG триггерится через unified_moex_pipeline
    default_args=args,
    tags=["dm", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    # Сенсор убран: будет триггер через unified_moex_pipeline (TriggerDagRunOperator)

    def _get_target_date(**context) -> str:
        conf = context.get("dag_run").conf if context.get("dag_run") else {}
        target_date = (conf or {}).get("target_date")
        if target_date:
            return str(target_date)
        return context["data_interval_start"].subtract(days=1).format("YYYY-MM-DD")

    # Upsert размерности через DuckDB Postgres extension
    def upsert_dim_bond_func(**context) -> None:
        con = duckdb.connect()
        try:
            con.execute("INSTALL postgres; LOAD postgres;")
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
            con.execute("ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);")
            # Сначала удаляем существующие записи для новых secid
            con.execute("""
                DELETE FROM dwh_postgres_db.dm.dim_bond
                WHERE secid IN (
                    SELECT DISTINCT secid 
                    FROM dwh_postgres_db.ods.dwh_bond 
                    WHERE secid IS NOT NULL AND secid <> ''
                );
            """)
            
            # Затем вставляем новые/обновленные записи
            con.execute("""
                INSERT INTO dwh_postgres_db.dm.dim_bond (secid, shortname, matdate, facevalue, currencyid, faceunit)
                SELECT DISTINCT
                  secid,
                  shortname,
                  matdate,
                  facevalue,
                  currencyid,
                  faceunit
                FROM dwh_postgres_db.ods.dwh_bond
                WHERE secid IS NOT NULL AND secid <> '';
            """)
            
            logging.info("✅ dm.dim_bond upsert done")
        finally:
            con.close()

    upsert_dim_bond = PythonOperator(
        task_id="upsert_dim_bond",
        python_callable=upsert_dim_bond_func,
    )

    # Перезагрузка факта за целевую дату (dag_run.conf.target_date) или вчера через DuckDB
    def load_fct_bond_day_func(**context) -> None:
        target_date = _get_target_date(**context)
        con = duckdb.connect()
        try:
            con.execute("INSTALL postgres; LOAD postgres;")
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
            con.execute("ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);")
            con.execute(
                f"DELETE FROM dwh_postgres_db.dm.fct_bond_day WHERE tradedate = '{target_date}'::date;"
            )
            con.execute(
                f"""
                INSERT INTO dwh_postgres_db.dm.fct_bond_day
                (tradedate, secid, numtrades, volume, value, low, high, close_avg, yieldclose_avg)
                SELECT
                  tradedate::date                                  AS tradedate,
                  secid,
                  COALESCE(SUM(numtrades), 0)                      AS numtrades,
                  COALESCE(SUM(volume), 0)                         AS volume,
                  COALESCE(SUM(value), 0)                          AS value,
                  MIN(low)                                         AS low,
                  MAX(high)                                        AS high,
                  AVG(close)                                       AS close_avg,
                  AVG(yieldclose)                                  AS yieldclose_avg
                FROM dwh_postgres_db.ods.dwh_bond
                WHERE tradedate::date = '{target_date}'::date
                GROUP BY 1,2;
                """
            )
        finally:
            con.close()

    load_fct_bond_day = PythonOperator(
        task_id="load_fct_bond_day",
        python_callable=load_fct_bond_day_func,
    )

    end = EmptyOperator(
        task_id="end",
    )

    (
            start >>
            upsert_dim_bond >>
            load_fct_bond_day >>
            end
    )