import logging
import datetime
import time

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def log(**kwargs):
    time.sleep(10)
    logging.info(f'passed: {kwargs["task"].task_id}')


with DAG(
        dag_id="test",
        schedule=datetime.timedelta(hours=4),
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["test"],
) as dag:
    list_task = [
        PythonOperator(
            task_id=f'task{i}',
            python_callable=log,
            provide_context=True,
        ) for i in range(10)]

    list_task[0] >> list_task[1] >> [list_task[2], list_task[3]]
    list_task[2] >> [list_task[4], list_task[5]]
    list_task[3] >> list_task[4]
    list_task[5]>>list_task[6:]
