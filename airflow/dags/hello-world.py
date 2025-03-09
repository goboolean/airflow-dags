from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 2, 1),
}

with DAG('test-dag', default_args=default_args, schedule_interval=None) as dag:
    task = KubernetesPodOperator(
        task_id='test-dag',
        name='test',
        namespace='fetch-system',
        image='alpine:latest',
        cmds=["sh", "-c"],
        arguments=["echo 'Hello World!' && exit 0"],
        is_delete_operator_pod=False,
        get_logs=True
    )
