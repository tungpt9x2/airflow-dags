from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta

import pandas as pd
import json
from datetime import datetime
import pytz

timezone = pytz.timezone('Asia/Bangkok')

args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=10),
    'retries': 1,
}

# defining the dag object
dag = DAG(
    dag_id='tsa-dag',
    default_args=args,
    schedule_interval=timedelta(minutes=10) # to make this workflow happen every 10 minutes
)

def loadExamPlanData():
    sql_query = '''
        SELECT
            exam_plan.billing_id,
            exam_plan.profile_id,
            exam_plan.test_site_id,
            exam_plan.exam_id,
            exam_plan.status,
            billing.invoice_total_amount
        FROM
            PUBLIC.exam_plan
            INNER JOIN orders.billing ON billing.ID = exam_plan.billing_id
        WHERE
            exam_id = 38
        '''
    pg_hook = PostgresHook(postgres_conn_id="student-portal-db")
    data = pg_hook.get_records(sql_query)
    return data


def loadProvinceData():
    sql_query = '''
        SELECT provinceid, name FROM address.province
        '''
    pg_hook = PostgresHook(postgres_conn_id="student-portal-db")
    data = pg_hook.get_records(sql_query)
    return data


def loadSchoolData():
    import requests

    r = requests.get('https://xcbt-dev-storage.apps.prod01.fis-cloud.fpt.com/xcbt-files/tmp/schools.json')
    return r.json()


# Map school
def schoolMap(hk1_10, hk2_10, hk1_11, hk2_11, hk1_12, hk2_12):
    if (pd.notna(hk2_12)):
        return hk2_12
    elif (pd.isna(hk2_12) & pd.notna(hk1_12)):
        return hk1_12
    elif (pd.isna(hk2_12) & pd.isna(hk1_12) & pd.notna(hk2_11)):
        return hk2_11
    elif (pd.isna(hk2_12) & pd.isna(hk1_12) & pd.isna(hk2_11) & pd.notna(hk1_11)):
        return hk1_11
    elif (pd.isna(hk2_12) & pd.isna(hk1_12) & pd.isna(hk2_11) & pd.isna(hk1_11) & pd.notna(hk2_10)):
        return hk2_10
    elif (pd.isna(hk2_12) & pd.isna(hk1_12) & pd.isna(hk2_11) & pd.isna(hk1_11) & pd.isna(hk2_10) & pd.notna(hk1_10)):
        return hk1_10

# Map province
def provinceMap(hk1_10, hk2_10, hk1_11, hk2_11, hk1_12, hk2_12, province):
    if (pd.isna(hk1_12) & pd.isna(hk2_12) & pd.isna(hk1_11) & pd.isna(hk2_11) & pd.isna(hk1_10) & pd.isna(hk2_10)):
        return province


def getProfileIds(ti):
    exam_plans = ti.xcom_pull(task_ids='load_exam_plan_data')
    exam_plan_df = pd.DataFrame(data=exam_plans, columns=[
                                "billing_id", "profile_id", "test_site_id", "exam_id", "status", "invoice_total_amount"])

    profile_ids = exam_plan_df['profile_id'].to_list()
    return profile_ids


def loadProfileData(ti):
    profile_ids = ti.xcom_pull(task_ids='get_profile_id')

    sql_query = '''
        SELECT id, data FROM public.profile where id IN %s
        '''
    pg_hook = PostgresHook(postgres_conn_id="student-portal-db")
    data = pg_hook.get_records(
        sql_query, parameters=(tuple(profile_ids),))

    return data

def loadTestSiteData(ti):
    sql_query = '''
        SELECT name, count_test_taker, max_test_taker FROM test_site WHERE exam_id = 38 ORDER BY name ASC
        '''
    pg_hook = PostgresHook(postgres_conn_id="student-portal-db")
    data = pg_hook.get_records(sql_query)

    return data


def processExamPlanData(ti):
    exam_plans = ti.xcom_pull(task_ids='load_exam_plan_data')
    schools = ti.xcom_pull(task_ids='load_school_data')
    provinces = ti.xcom_pull(task_ids='load_province_data')
    profiles = ti.xcom_pull(task_ids='load_profile_data')
    test_sites = ti.xcom_pull(task_ids='load_test_site_data')

    exam_plan_df = pd.DataFrame(data=exam_plans, columns=[
        "billing_id", "profile_id", "test_site_id", "exam_id", "status", "invoice_total_amount"])

    province_df = pd.DataFrame(data=provinces, columns=[
        "provinceid", "name"])

    profile_df = pd.DataFrame(data=profiles, columns=[
        "id", "data"])

    school_df = pd.DataFrame(data=schools)

    test_site_df = pd.DataFrame(data=test_sites, columns=[
        "name", "count_test_taker", "max_test_taker"])

    exam_plan_extra_df = pd.concat(
        [exam_plan_df, profile_df['data'].apply(pd.Series)], axis=1)

    # Lấy Id tỉnh từ thông tin nơi học THPT
    exam_plan_extra_df['province_id1'] = exam_plan_extra_df.apply(lambda x: schoolMap(x['province_10_hk1'], x['province_10_hk2'], x['province_11_hk1'],
                                                                                      x['province_11_hk2'], x['province_12_hk1'], x['province_12_hk2']), axis=1)

    exam_plan_extra_df['province_name1'] = exam_plan_extra_df['province_id1'].map(
        dict(zip(school_df['city_code'], school_df['city_name'])))

    # Lấy Id tỉnh từ thông tin cư trú
    exam_plan_extra_df['province_id2'] = exam_plan_extra_df.apply(lambda x: provinceMap(x['province_10_hk1'], x['province_10_hk2'], x['province_11_hk1'],
                                                                                        x['province_11_hk2'], x['province_12_hk1'], x['province_12_hk2'], x['province']), axis=1)

    exam_plan_extra_df['province_name2'] = exam_plan_extra_df['province_id2'].map(
        dict(zip(province_df['provinceid'], province_df['name'])))

    exam_plan_extra_df['province_name'] = exam_plan_extra_df['province_name1'].fillna(
        '') + exam_plan_extra_df['province_name2'].fillna('')

    register_by_province = exam_plan_extra_df.groupby(
        ['province_name', 'status']).size().reset_index(name='count')
    register_by_province['status'] = register_by_province['status'].map({0: 'Chưa thanh toán', 1: 'Đã thanh toán'})
    register_by_province = register_by_province.sort_values(['count'], ascending=False)

    # Tổng số đăng ký
    total_registered = exam_plan_extra_df.shape[0]
    # Tổng số tiền
    total_amount = exam_plan_extra_df['invoice_total_amount'].sum()
    
    exam_plan_extra_paid_df = exam_plan_extra_df[exam_plan_extra_df['status'] == 1]
    # Số đăng ký đã thanh toán
    total_paid_registered = exam_plan_extra_paid_df.shape[0]
    # Số tiền đã thanh toán
    total_paid_amount = exam_plan_extra_paid_df['invoice_total_amount'].sum()

    result = {
        "total_registered": int(total_registered),
        "total_amount": int(total_amount),
        "total_paid_registered": int(total_paid_registered),
        "total_paid_amount": int(total_paid_amount),
        "register_by_province": register_by_province.to_json(orient="records"),
        "resgister_by_test_site": test_site_df.to_json(orient="records"),
        "last_updated_at": int(round(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(timezone).timestamp()))
    }

    print(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(timezone))
    print(int(round(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(timezone).timestamp())))

    return result

def saveReport(ti):
    import redis
    report_data = ti.xcom_pull(task_ids='process_exam_plan_data')

    redisConnection = redis.StrictRedis(
            host=Variable.get("REDIS_HOST"),
            port=Variable.get("REDIS_PORT"),
            db=0,
            password=Variable.get("REDIS_PASSWORD"),
        )

    redisConnection.set(
        "AuthoringCache:TSA_REGISTER_REPORT", json.dumps(report_data)
    )


with dag:
    load_exam_plan_data = PythonOperator(
        task_id='load_exam_plan_data',
        python_callable=loadExamPlanData,
    )

    load_province_data = PythonOperator(
        task_id='load_province_data',
        python_callable=loadProvinceData,
    )

    load_school_data = PythonOperator(
        task_id='load_school_data',
        python_callable=loadSchoolData,
    )

    get_profile_id = PythonOperator(
        task_id='get_profile_id',
        python_callable=getProfileIds,
    )

    load_profile_data = PythonOperator(
        task_id='load_profile_data',
        python_callable=loadProfileData,
    )

    load_test_site_data = PythonOperator(
        task_id='load_test_site_data',
        python_callable=loadTestSiteData,
    )

    process_exam_plan_data = PythonOperator(
        task_id='process_exam_plan_data',
        python_callable=processExamPlanData,
    )

    save_report = PythonOperator(
        task_id='save_report',
        python_callable=saveReport,
    )

    [load_exam_plan_data, load_school_data,
        load_province_data] >> get_profile_id >> [load_profile_data, load_test_site_data] >> process_exam_plan_data >> save_report


if __name__ == "__main__":
    dag.test()
