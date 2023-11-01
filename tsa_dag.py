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


def queryExamPlanData():
    sql_query = '''
        SELECT exam_plan.*, billing.invoice_total_amount FROM public.exam_plan JOIN orders.billing ON billing.id = exam_plan.billing_id WHERE exam_id = 38
        '''
    pg_hook = PostgresHook(postgres_conn_id="student-portal-db")
    return pg_hook.get_records(sql_query)


with dag:
    load_exam_plan_data = PythonOperator(
        task_id='load_exam_plan_data',
        python_callable=queryExamPlanData,
    )

    load_exam_plan_data

if __name__ == "__main__":
    dag.test()
