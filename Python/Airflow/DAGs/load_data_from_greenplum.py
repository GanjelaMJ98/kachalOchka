"""
Загружаем данные из GreenPlum
"""

import logging
from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2), # дата на 1 больше дня начала и конца
    'end_date': datetime(2022, 3, 15),
    'owner': 'p_ganzhela',
    'poke_interval': 600
}

with DAG("p_ganzhela_get_data",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p-ganzhela-11', 'pavlusha']
         ) as dag:

    
    def is_not_weekend_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day not in [5,6]

    def get_value_from_greenplum_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        my_sql = f"SELECT heading FROM articles WHERE id = {exec_day}"
        logging.info(f'I have sql : {my_sql}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')   # инициализируем хук
        conn = pg_hook.get_conn()                                   # берём из него соединение
        cursor = conn.cursor("named_cursor_name")                   # и именованный (необязательно) курсор
        cursor.execute(my_sql)                                      # исполняем sql
        query_res = cursor.fetchall()                               # полный результат
        logging.info(f'I have value : {query_res}')


    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    not_weekend = ShortCircuitOperator(
        task_id='not_weekend',
        python_callable=is_not_weekend_func,
        op_kwargs = {'execution_dt' : '{{ ds }}'}
    )

    get_value_from_greenplum = PythonOperator(
        task_id='get_value_from_greenplum',
        python_callable=get_value_from_greenplum_func,
        op_kwargs = {'execution_dt' : '{{ ds }}'}
    )


    
    echo_ds >> not_weekend >> get_value_from_greenplum
