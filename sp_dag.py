from airflow.models.dag import DAG
from airflow.utils.dates import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


from datetime import datetime

args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=5),
    'retries': 1,
}

dag = DAG(
    dag_id='sp-dag',
    default_args=args,
    # to make this workflow happen every 5 minutes
    schedule_interval=timedelta(minutes=5)
)


def excuteReport():
    pg_hook = PostgresHook(postgres_conn_id="authoring-p3-db")
    cursor = pg_hook.get_conn().cursor()

    cursor.excute("CALL report.sp_report_update_result();")


with dag:
    excute_report_task = PythonOperator(
        task_id='excute_report_task',
        python_callable=excuteReport,
    )

    excute_report_task


if __name__ == "__main__":
    dag.test()
