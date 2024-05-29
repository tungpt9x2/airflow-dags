from airflow.models.dag import DAG
from airflow.utils.dates import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from datetime import datetime


args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=60),
    'retries': 1,
}

dag = DAG(
    dag_id='edl-dag',
    default_args=args,
    schedule_interval=timedelta(minutes=60) # to make this workflow happen every 10 minutes
)

def countExamStatistic():
    redis_hook = RedisHook(redis_conn_id="authoring-redis")
    conn = redis_hook.get_conn()
    data_len = conn.llen("AuthoringCache:EXAM_STATISTICS:28052024")
    return data_len

def getExamStatistic(ti):
    data_len = ti.xcom_pull(task_ids='count_data')
    redis_hook = RedisHook(redis_conn_id="authoring-redis")
    conn = redis_hook.get_conn()
    data = conn.lrange("AuthoringCache:EXAM_STATISTICS:28052024", 0 , data_len - 1)
    return data

with dag:
    count_data = PythonOperator(
            task_id='count_data',
            python_callable=countExamStatistic,
    )

    get_data = PythonOperator(
            task_id='get_data',
            python_callable=getExamStatistic,
    )

    count_data >> get_data

if __name__ == "__main__":
    dag.test()