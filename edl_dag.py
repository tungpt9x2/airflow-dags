from airflow.models.dag import DAG
from airflow.utils.dates import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.S3_hook import S3Hook

from datetime import datetime
import pandas as pd
import json
from io import BytesIO


args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=240),
    'retries': 1,
}

dag = DAG(
    dag_id='edl-dag',
    default_args=args,
    schedule_interval=timedelta(minutes=240) # to make this workflow happen every 4 hours
)

def countExamStatistic():
    redis_hook = RedisHook(redis_conn_id="authoring-redis")
    conn = redis_hook.get_conn()
    data_len = conn.llen("AuthoringCache:EXAM_STATISTICS:"+datetime.now().strftime("%d%m%Y"))
    return data_len

def getExamStatistic(ti):
    data_len = ti.xcom_pull(task_ids='count_data')
    redis_hook = RedisHook(redis_conn_id="authoring-redis")
    conn = redis_hook.get_conn()
    data = conn.lrange("AuthoringCache:EXAM_STATISTICS:" + datetime.now().strftime("%d%m%Y"), 0 , data_len - 1)

    final_data = []
    for x in data:
        final_data.append(json.loads(x))

    final_data_df = pd.DataFrame(data=final_data)
    csv_buffer = BytesIO()
    final_data_df.to_csv(csv_buffer, header=True, index=False, encoding='utf-8', date_format='%Y-%m-%dT%H:%M:%S.%fZ')

    s3_hook = S3Hook(aws_conn_id='minio-admin')
    bucket_name = "airflow-logs"    
    s3_hook.load_bytes(csv_buffer.getvalue(), f"exam_statistics_{datetime.now()}.csv", bucket_name)

# def list_keys():
#     hook = S3Hook(aws_conn_id='minio-admin')
#     bucket = "airflow-logs"
#     keys = hook.list_keys(bucket, prefix="")
#     for key in keys:
#         print(f"- s3://{bucket}/{key}")

with dag:
    count_data = PythonOperator(
            task_id='count_data',
            python_callable=countExamStatistic,
    )

    process_data = PythonOperator(
            task_id='process_data',
            python_callable=getExamStatistic,
    )

    # list_task = PythonOperator(
    #     task_id="list_keys",
    #     python_callable=list_keys,
    # )

    count_data >> process_data
    # list_task

if __name__ == "__main__":
    dag.test()