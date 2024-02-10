from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 
from dag_sensitive_config import NOTEBOOK_PATH, CLUSTER_ID, NOTEBOOK_PARAMS, DATABRICKS_CONN_ID


#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': NOTEBOOK_PATH,
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': 'Spencer Duvwiama',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


with DAG('0a1667ad2f7f_dag',
    # should be a datetime format
    start_date=datetime(2024, 2, 11),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id=DATABRICKS_CONN_ID,
        existing_cluster_id=CLUSTER_ID,
        notebook_task=notebook_task
    )
    opr_submit_run