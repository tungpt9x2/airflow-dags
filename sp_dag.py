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


def excuteReportP3():
    pg_hook = PostgresHook(postgres_conn_id="authoring-p3-db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("CALL report.sp_report_update_result();")
        cursor.execute("CALL report.sp_calculate_report_test_taker_group();")
        cursor.execute("CALL report.sp_report_update_result();")
        cursor.execute("CALL report.sp_calculate_report_class();")
    except:
        cursor.execute("ROLLBACK")
    cursor.close()
    conn.commit()
    conn.close()


def excuteReportP1():
    pg_hook = PostgresHook(postgres_conn_id="authoring-p1-db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("CALL report.sp_report_update_result();")
        cursor.execute("CALL report.sp_calculate_report_test_taker_group();")
        cursor.execute("CALL report.sp_report_update_result();")
        cursor.execute("CALL report.sp_calculate_report_class();")
    except:
        cursor.execute("ROLLBACK")
    cursor.close()
    conn.commit()
    conn.close()


with dag:
    excute_report_p3 = PythonOperator(
        task_id='excute_report_p3',
        python_callable=excuteReportP3,
    )

    excute_report_p1 = PythonOperator(
        task_id='excute_report_p1',
        python_callable=excuteReportP1,
    )

    excute_report_p3 >> excute_report_p1


if __name__ == "__main__":
    dag.test()
