import os
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
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
        dag_id="polygon_s3_to_gcs",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,  # 수동 트리거 전용
        catchup=False,
) as dag:
    # 시작과 종료 더미 태스크
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # dag_run.conf에서 start_year와 end_year 가져오기 (입력 필수)
    dagrun = dag.get_dagrun()
    if dagrun is None or "start_year" not in dagrun.conf or "end_year" not in dagrun.conf:
        raise ValueError(
            "Both 'start_year' and 'end_year' must be provided in the DAG run configuration (e.g., {'start_year': 2015, 'end_year': 2025})"
        )

    start_year = dagrun.conf["start_year"]
    end_year = dagrun.conf["end_year"]

    # 입력 검증
    if not isinstance(start_year, int) or not isinstance(end_year, int):
        raise ValueError("'start_year' and 'end_year' must be integers")
    if start_year > end_year:
        raise ValueError("'start_year' must be less than or equal to 'end_year'")

    # years 범위 생성 (end_year 포함)
    years = list(range(start_year, end_year + 1))

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
                arguments=["/app/polygon_to_gcs_batch.sh", year_str],  # year를 인자로 전달
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
    # TODO: polygon_to_gcs >> split_ticker_to_resample_bucket >> make_several_resampled_data >> insert_to_influxDB
