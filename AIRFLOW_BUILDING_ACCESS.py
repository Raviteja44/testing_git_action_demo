# author: Lluis Dolade
# version: 2025.1
# description: This Airflow DAG creates "bec_dwh"."building_access_management" table and loads data incrementally
# File Version: v1.0

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from redshift_utils import redshift_connection as rs

# Static Variables
PWD = Variable.get('password_ctc')
USERNAME = Variable.get('username_ctc')
DATABASE = Variable.get('database')
HOST = Variable.get('host')
PORT = int(Variable.get('port'))
MAIL_ID = Variable.get('mail_id')
AWS_REGION = Variable.get('aws_region')

# DAG Configuration
WORKFLOW_DAG_ID = 'BUILDING_ACCESS'
WORKFLOW_START_DATE = datetime(2025, 1, 1)

DWH_CONNECTION_INFO = {
    "username": USERNAME,
    "password": PWD,
    "host": HOST,
    "port": PORT,
    "database": DATABASE
}

def check_table_exists(**context):
    rsconn = rs()
    check_sql = """
    SELECT COUNT(1) 
    FROM information_schema.tables 
    WHERE table_schema = 'bec_dwh'
    AND table_name = 'building_access_management';
    """
    result = rsconn.execute_query(DWH_CONNECTION_INFO, AWS_REGION, check_sql)
    return result[0][0] if result else 0

def create_view(**context):
    rsconn = rs()
    create_view_sql = """
    CREATE OR REPLACE VIEW "bec_dwh"."v_building_access_management" AS 
    WITH attendance_agg AS (
        SELECT 
            employee_id
        , converted_dt
        , MIN(first_converted_ts) first_occurred_ts
        , max(last_updated_date_pst) last_updated_date_pst
        FROM "ext_be_edp_emp_details"."f_brivo"
        GROUP BY employee_id, converted_dt
    ),
    base_query AS (
        SELECT 
            c.date,
            c.employeeid,
            c.st,
            c.is_workingday,
            CASE 
                WHEN c.is_workingday = 0 THEN 0 
                ELSE c.on_loa
            END as on_loa,
            l.laststatus,
            CASE 
                WHEN l.laststatus IS NULL THEN 0 
                ELSE l.on_leave 
            END as on_leave,
            l.leavetype,
            CASE 
                WHEN a.converted_dt IS NOT NULL AND a.employee_id IS NOT NULL THEN 1
                ELSE 0
            END as is_attended,
            a.first_occurred_ts as occurred_ts,
            a.last_updated_date_pst
        FROM "ext_be_edp_emp_details"."d_calendar" c
        LEFT JOIN "ext_be_edp_emp_details"."d_adp" l 
            ON c.date::date = l.date 
            AND c.mail = l.email
        LEFT JOIN attendance_agg a 
            ON c.date::date = a.converted_dt 
            AND c.employeeid = a.employee_id
        WHERE c.date >= '2025-01-01'
            AND c.date::timestamp <= CONVERT_TIMEZONE('America/Los_Angeles', GETDATE())::date
    )
    SELECT 
        b.*,
        ad.c,
        ad.physicaldeliveryofficename,
        ad.streetaddress,
        ad.displayname,
        ad.employeetype,
        ad.department,
        ad.manager,
        ad.executive,
        ad.seniorexecutive,
        ad.remote,
        ad.status
    FROM base_query b
    LEFT JOIN "ext_be_edp_emp_details"."d_ad" ad
        ON b.employeeid = ad.employeeid
    WHERE ad.status = 'active' 
        AND ad.employeetype in ('Regular', 'regular')
        AND ad.start_date <= CONVERT_TIMEZONE('America/Los_Angeles', GETDATE())::date
        with no schema binding;
    """
    rsconn.execute_query(DWH_CONNECTION_INFO, AWS_REGION, create_view_sql)

def create_initial_table(**context):
    rsconn = rs()
    create_table_sql = """
    BEGIN;
    DROP TABLE IF EXISTS "bec_dwh"."building_access_management";
    CREATE TABLE "bec_dwh"."building_access_management" AS 
    SELECT * FROM "bec_dwh"."v_building_access_management";
    COMMIT;
    """
    rsconn.execute_query(DWH_CONNECTION_INFO, AWS_REGION, create_table_sql)

def incremental_load(**context):
    rsconn = rs()
    incremental_sql = """
    BEGIN;
    INSERT INTO "bec_dwh"."building_access_management"
    SELECT * FROM "bec_dwh"."v_building_access_management"
    WHERE last_updated_date_pst > (SELECT MAX(last_updated_date_pst) FROM "bec_dwh"."building_access_management");
    COMMIT;
    """
    rsconn.execute_query(DWH_CONNECTION_INFO, AWS_REGION, incremental_sql)

def manage_table_load(**context):
    try:
        rsconn = rs()
        create_view(**context)
        
        if not check_table_exists(**context):
            create_initial_table(**context)
        else:
            incremental_load(**context)
            
    except Exception as e:
        context['task_instance'].xcom_push(key='error_details', value=str(e))
        raise

def dag_success(**context):
    """Log success message"""
    print("DAG completed successfully")
    return 'Success'

def dag_error(**context):
    """Handle error and log error details"""
    error_message = context['task_instance'].xcom_pull(key='error_details')
    print(f"DAG failed with error: {error_message}")
    return 'Failed'

dag = DAG(
    dag_id='BUILDING_ACCESS',
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 17,18,20,23 * * *',
    default_args={
        'owner': 'BE-EDP',
        'depends_on_past': False,
        'email': MAIL_ID,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    catchup=False,
    max_active_runs=1
)

run_glue_job1 = GlueJobOperator(
    task_id='glue-job-expected',
    job_name='be-edp-attendance-tracking-expected',
    region_name=AWS_REGION,
    wait_for_completion=True,
    dag=dag
)

run_glue_job2 = GlueJobOperator(
    task_id='glue-job-attended',
    job_name='be-edp-attendance-tracking-attended',
    region_name=AWS_REGION,
    wait_for_completion=True,
    dag=dag
)

run_glue_job3 = GlueJobOperator(
    task_id='glue-job-attended-join',
    job_name='be-edp-attendance-tracking-attended-join',
    region_name=AWS_REGION,
    wait_for_completion=True,
    dag=dag
)

manage_load = PythonOperator(
    task_id='manage_table_load',
    python_callable=manage_table_load,
    provide_context=True,
    dag=dag
)

success_task = PythonOperator(
    task_id='load_success',
    python_callable=dag_success,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag
)

error_task = PythonOperator(
    task_id='load_error',
    python_callable=dag_error,
    provide_context=True,
    trigger_rule='one_failed',
    dag=dag
)

# Define the task dependencies - Glue jobs run first, then Redshift load
run_glue_job1 >> run_glue_job2 >> run_glue_job3 >> manage_load >> [success_task, error_task]
