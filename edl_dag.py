from airflow.models.dag import DAG
from airflow.utils.dates import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from datetime import datetime


args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=10),
    'retries': 1,
}

dag = DAG(
    dag_id='edl-dag',
    default_args=args,
    schedule_interval=timedelta(minutes=10) # to make this workflow happen every 10 minutes
)

def loadExamStatistic():
    redis_hook = RedisHook(redis_conn_id="authoring-redis")
    data = redis_hook.llen("AuthoringCache:EXAM_STATISTICS:28052024")
    print(data)
    return data
with dag:
    load_data = PythonOperator(
            task_id='load_data',
            python_callable=loadExamStatistic,
    )

    load_data

if __name__ == "__main__":
    dag.test()