# batch_pipeline.py
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes import KubernetesPodOperator
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수 가져오기
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
GCS_PROJECT_ID = os.getenv("GCS_PROJECT_ID")

# 필수 환경 변수 검증
required_vars = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "GCS_PROJECT_ID": GCS_PROJECT_ID,
}
missing_vars = [key for key, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# DAG 정의
with DAG(
    dag_id="polygon_s3_to_gcs_daily",
    start_date=datetime(2025, 3, 1),  # 시작 날짜 (2025년 3월 1일부터 유효)
    schedule_interval="0 0 * * *",   # 매일 00:00에 실행 (Cron 형식)
    catchup=False,                   # 과거 실행 건너뜀
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    # 시작과 종료 더미 태스크
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # 실행 날짜 기준 이틀 전 날짜를 YEAR, MONTH, DAY로 분리
    year = "{{ (execution_date - macros.timedelta(days=2)).strftime('%Y') }}"
    month = "{{ (execution_date - macros.timedelta(days=2)).strftime('%m') }}"
    day = "{{ (execution_date - macros.timedelta(days=2)).strftime('%d') }}"

    # KubernetesPodOperator로 태스크 정의
    polygon_to_gcs = KubernetesPodOperator(
        task_id="transfer_daily_data",
        name="polygon-transfer-daily",
        namespace="data-system",
        image="polygon_fetcher:test",  # 커스텀 이미지
        cmds=["bash"],
        arguments=[
            "/app/polygon_to_gcs_batch.sh",
            year,  # YEAR 전달
            month,  # MONTH 전달
            day,   # DAY 전달
        ],
        env_vars={
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "GCS_PROJECT_ID": GCS_PROJECT_ID
        },
        service_account_name="gcs-service-account",
        get_logs=True,
        is_delete_pod=True,
    )

    # DAG 흐름 정의
    start >> polygon_to_gcs >> end
