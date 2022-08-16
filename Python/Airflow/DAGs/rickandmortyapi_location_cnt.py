"""
Загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from p_ganzhela_plugins.p_ganzhela_location_count_operator import GanzhelaLocationCountOperator



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'p_ganzhela',
    'poke_interval': 600
}


with DAG("p_ganzhela_location_cnt",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p_ganzhela']
         ) as dag:

    start = DummyOperator(task_id='start')

    def create_db_table_func():
        my_sql = """CREATE TABLE IF NOT EXISTS p_ganzhela_11_ram_location (
                    id INT,
                    name TEXT,
                    type TEXT,
                    dimension TEXT,
                    resident_cnt INT);"""
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(my_sql, False)


    create_db_table = PythonOperator(
        task_id='create_db_table',
        python_callable=create_db_table_func
    )

    insert_location_cnt_to_db = GanzhelaLocationCountOperator(
        task_id='insert_location_cnt_to_db'
    )


    start >> create_db_table >> insert_location_cnt_to_db
