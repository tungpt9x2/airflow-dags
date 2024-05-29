from airflow.models.dag import DAG
from airflow.utils.dates import timedelta
from airflow.providers.redis.operators.redis_publish import RedisPublishOperator
from airflow.providers.redis.sensors.redis_key import RedisKeySensor


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

key_sensor_task = RedisKeySensor(
        task_id="key_sensor_task",
        redis_conn_id="authoring-redis",
        key="AuthoringCache:EXAM_STATISTICS:28052024",
        dag=dag,
        timeout=600,
        poke_interval=30,
    )


key_sensor_task

if __name__ == "__main__":
    dag.test()