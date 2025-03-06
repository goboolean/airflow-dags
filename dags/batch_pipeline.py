import os
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수 설정
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PATH = os.getenv("S3_PATH")
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_PATH = os.getenv("GCS_PATH")

# 필수 환경 변수 체크
required_vars = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "GOOGLE_CREDENTIALS": GOOGLE_CREDENTIALS,
    "S3_BUCKET": S3_BUCKET,
    "S3_PATH": S3_PATH,
    "GCS_BUCKET": GCS_BUCKET,
    "GCS_PATH": GCS_PATH,
}
missing_vars = [key for key, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# DAG 정의
with DAG(
    dag_id="polygon_s3_to_gcs",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # 스케줄 비활성화로 수동 트리거 전용
    catchup=False,
) as dag:
    # 시작과 종료 더미 태스크
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # 10년치 데이터 (2015~2024)
    years = range(2015, 2025)

    with TaskGroup("polygon_to_gcs") as polygon_to_gcs:
        for year in years:
            task_id = f"transfer_{year}"
            year_str = str(year)

            # KubernetesPodOperator로 태스크 정의
            task = KubernetesPodOperator(
                task_id=task_id,
                name=f"polygon-transfer-{year}",
                namespace="data-system",
                image="polygon_fetcher:test",  # 커스텀 이미지
                cmds=["bash"],
                arguments=["/app/polygon_to_gcs_batch.sh"],  # Bash 스크립트 경로
                env_vars={
                    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                    "GOOGLE_CREDENTIALS": GOOGLE_CREDENTIALS,
                    "YEAR": year_str,
                    "S3_BUCKET": S3_BUCKET,
                    "S3_PATH": S3_PATH,
                    "GCS_BUCKET": GCS_BUCKET,
                    "GCS_PATH": GCS_PATH,
                },
                get_logs=True,  # Pod 로그를 Airflow UI에서 확인 가능
                is_delete_pod=True,  # 작업 완료 후 Pod 삭제
                # TODO: 리소스 제한 설정 가능 (예: resources={"requests": {"cpu": "500m", "memory": "1Gi"}})
            )

    # DAG 흐름 정의
    start >> polygon_to_gcs >> end
    # TODO: polygon_to_gcs >> split_ticker_to_resample_bucket >> make_several_resampled_data >> insert_to_influxDB
