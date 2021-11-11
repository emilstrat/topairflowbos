from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.time_delta_sensor import TimeDeltaSensor 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from urllib.parse import urlencode

import sys
sys.path.append("/root/airflow/dags/operators/")

from ms_teams_webhook_operator import MSTeamsWebhookOperator

##Fetch variable values
var_combi_warehouse = Variable.get ("COMBI_WAREHOUSE")                          #ändra till combi
var_snowflake_database = Variable.get ("SNOWFLAKE_DATABASE")

#Change sleeping time on TimeDeltaSensors. Wait two days before running load/export. 
# Add more timers if different activation dates are needed
var_usage_report_sleep_days = 2

#Functions
def on_failure(context):
    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id
    context['task_instance'].xcom_push(key=dag_id, value=True)
    exec_date=context.get("execution_date")
    pay_load = { 'task_id' : task_id, 'dag_id' : dag_id,'execution_date' : exec_date,'format' : 'json' }
    logs_url= 'https://airflow-prod.tdh.tele2.net/admin/airflow/log?' + urlencode(pay_load)


def get_to_date():
    return '{{ ds }}'

def get_report_year():
    return '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}'

default_args = {
    "start_date": datetime(2020, 1, 1),
    "retries": 0,
    "pool":"default_pool",                           
    "on_failure_callback":on_failure
}

################################################################################################################################################
#DAG
################################################################################################################################################
dag = DAG("TDH_EXPORT_COMBI_DAILY", default_args=default_args, max_active_runs=1, schedule_interval="0 0 * * *", catchup=False, description="Workflow for the combi data exports", tags=["COMBI"])

################################################################################################################################################
#Sensors
################################################################################################################################################
CHECK_SIGMA_SE = ExternalTaskSensor(task_id="CHECK_SIGMA_SE", external_dag_id="TDH_LOAD_STAGE_AND_RAW_DAILY_EXTENDED", external_task_id="END_SIGMA_SE", poke_interval=900, timeout=9*3600, mode="reschedule", dag=dag)

################################################################################################################################################
#TV_EXPORT_MONTHLY Tasks
################################################################################################################################################
#Dummy tasks
SIGMA_SE_RAW_DONE       = DummyOperator(task_id='SIGMA_SE_RAW_DONE', dag=dag)
SIGMA_SE_EXPORT_DONE    = DummyOperator(task_id='SIGMA_SE_EXPORT_DONE', dag=dag)
EXPORT_COMBI_DONE       = DummyOperator(task_id='EXPORT_COMBI_DONE', dag=dag)

#Export tasks
USP_EXPORT_COMBI_ACCESS_SERVICES        = SnowflakeOperator (task_id="USP_EXPORT_COMBI_ACCESS_SERVICES", sql="CALL TDH_METADATA.USP_RUN_PROCEDURE('EXPORT.USP_EXPORT_COMBI_ACCESS_SERVICES', '''" + get_to_date() + "''');", snowflake_conn_id="SNOWFLAKE_CONNECTION", warehouse=var_combi_warehouse, database=var_snowflake_database, depends_on_past=True, dag=dag)
USP_EXPORT_COMBI_ACCESSES               = SnowflakeOperator (task_id="USP_EXPORT_COMBI_ACCESSES", sql="CALL TDH_METADATA.USP_RUN_PROCEDURE('EXPORT.USP_EXPORT_COMBI_ACCESSES', '''" + get_to_date() + "''');", snowflake_conn_id="SNOWFLAKE_CONNECTION", warehouse=var_combi_warehouse, database=var_snowflake_database, depends_on_past=True, dag=dag)
USP_EXPORT_COMBI_CUSTOMERS              = SnowflakeOperator (task_id="USP_EXPORT_COMBI_CUSTOMERS", sql="CALL TDH_METADATA.USP_RUN_PROCEDURE('EXPORT.USP_EXPORT_COMBI_CUSTOMERS', '''" + get_to_date() + "''');", snowflake_conn_id="SNOWFLAKE_CONNECTION", warehouse=var_combi_warehouse, database=var_snowflake_database, depends_on_past=True, dag=dag)
USP_EXPORT_COMBI_CUSTOMER_SERVICES      = SnowflakeOperator (task_id="USP_EXPORT_COMBI_CUSTOMER_SERVICES", sql="CALL TDH_METADATA.USP_RUN_PROCEDURE('EXPORT.USP_EXPORT_COMBI_CUSTOMER_SERVICES', '''" + get_to_date() + "''');", snowflake_conn_id="SNOWFLAKE_CONNECTION", warehouse=var_combi_warehouse, database=var_snowflake_database, depends_on_past=True, dag=dag)
USP_EXPORT_COMBI_PROVISIONINGLOGS       = SnowflakeOperator (task_id="USP_EXPORT_COMBI_PROVISIONINGLOGS", sql="CALL TDH_METADATA.USP_RUN_PROCEDURE('EXPORT.USP_EXPORT_COMBI_PROVISIONINGLOGS', '''" + get_to_date() + "''');", snowflake_conn_id="SNOWFLAKE_CONNECTION", warehouse=var_combi_warehouse, database=var_snowflake_database, depends_on_past=True, dag=dag)
USP_EXPORT_COMBI_VILLAFIBER_ADDRESSES   = SnowflakeOperator (task_id="USP_EXPORT_COMBI_VILLAFIBER_ADDRESSES", sql="CALL TDH_METADATA.USP_RUN_PROCEDURE('EXPORT.USP_EXPORT_COMBI_VILLAFIBER_ADDRESSES', '''" + get_to_date() + "''');", snowflake_conn_id="SNOWFLAKE_CONNECTION", warehouse=var_combi_warehouse, database=var_snowflake_database, depends_on_past=True, dag=dag)


################################################################################################################################################
#Dependencies
################################################################################################################################################
CHECK_SIGMA_SE                          >> SIGMA_SE_RAW_DONE
SIGMA_SE_RAW_DONE                       >> USP_EXPORT_COMBI_ACCESS_SERVICES
SIGMA_SE_RAW_DONE                       >> USP_EXPORT_COMBI_ACCESSES
SIGMA_SE_RAW_DONE                       >> USP_EXPORT_COMBI_CUSTOMERS
SIGMA_SE_RAW_DONE                       >> USP_EXPORT_COMBI_CUSTOMER_SERVICES
SIGMA_SE_RAW_DONE                       >> USP_EXPORT_COMBI_PROVISIONINGLOGS
SIGMA_SE_RAW_DONE                       >> USP_EXPORT_COMBI_VILLAFIBER_ADDRESSES
USP_EXPORT_COMBI_ACCESS_SERVICES        >> SIGMA_SE_EXPORT_DONE
USP_EXPORT_COMBI_ACCESSES               >> SIGMA_SE_EXPORT_DONE
USP_EXPORT_COMBI_CUSTOMERS              >> SIGMA_SE_EXPORT_DONE
USP_EXPORT_COMBI_CUSTOMER_SERVICES      >> SIGMA_SE_EXPORT_DONE
USP_EXPORT_COMBI_PROVISIONINGLOGS       >> SIGMA_SE_EXPORT_DONE
USP_EXPORT_COMBI_VILLAFIBER_ADDRESSES   >> SIGMA_SE_EXPORT_DONE
SIGMA_SE_EXPORT_DONE                    >> EXPORT_COMBI_DONE