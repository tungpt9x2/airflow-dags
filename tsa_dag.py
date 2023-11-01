from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'airflow',
    'start_date': days_ago(1)  # make start date in the past
}

args = {
    'owner': 'airflow',
    'start_date': days_ago(1)  # make start date in the past
}

# defining the dag object
dag = DAG(
    dag_id='tsa-dag',
    default_args=args,
    schedule_interval='@daily'  # to make this workflow happen every day
)


def loadExamPlanData():
    from datetime import datetime
    import json

    sql_query = '''
        SELECT exam_plan.*, billing.invoice_total_amount FROM public.exam_plan JOIN orders.billing ON billing.id = exam_plan.billing_id WHERE exam_id = 38
        '''
    pg_hook = PostgresHook(postgres_conn_id="student-portal-db")
    data = pg_hook.get_records(sql_query)

    # Convert datetime objects to strings before serializing to JSON
    def convert_to_serializable(item):
        if isinstance(item, datetime):
            return item.isoformat()
        else:
            return item

    return json.dumps(data, default=convert_to_serializable)


def loadProvinceData():
    import json

    sql_query = '''
        SELECT * FROM address.province
        '''
    pg_hook = PostgresHook(postgres_conn_id="student-portal-db")
    data = pg_hook.get_records(sql_query)
    return json.dumps(data)


def loadProvinceJson():
    return ''


def processExamPlanData(ti):
    import pandas as pd
    exam_plans = ti.xcom_pull(task_ids='load_exam_plan_data')
    print(exam_plans)
#     return ''


with dag:
    load_exam_plan_data = PythonOperator(
        task_id='load_exam_plan_data',
        python_callable=loadExamPlanData,
    )

    load_province_data = PythonOperator(
        task_id='load_province_data',
        python_callable=loadProvinceData,
    )

    process_exam_plan_data = PythonOperator(
        task_id='process_exam_plan_data',
        python_callable=processExamPlanData,
    )

    [load_exam_plan_data, load_province_data] >> process_exam_plan_data

if __name__ == "__main__":
    dag.test()
