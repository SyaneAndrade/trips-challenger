from airflow import DAG
import airflow
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from utils.config import Config


def get_config():
    config = Config("/opt/airflow/config/dag_ingestions_csv.yaml")
    return config.get_config()

def create_dag(dag_id):

    dag = DAG(
        dag_id=dag_id, 
        start_date=airflow.utils.dates.days_ago(1),
        schedule_interval="@daily",
        catchup=False)
    return dag

def create_start(dag):
    return DummyOperator(
        task_id="start", 
        dag=dag)

def create_finish(dag):
    return DummyOperator(
        task_id="finish", 
        dag=dag)

def create_spark_submit(dag, config):
    return SparkSubmitOperator(task_id=f'spark_submit_{config["id"]}', 
                            application=config["application"],
                            name=config["name"],
                            conn_id=config["conn"],
                            application_args=[config["file_path"],
                                              config["spark_master"],
                                              config["db"]],
                            dag=dag)

def create_pipeline(dag_id, config):
    dag = create_dag(dag_id)
    start = create_start(dag)
    spark_submit = create_spark_submit(dag, config)
    finish = create_finish(dag)
    (
        start >>
        spark_submit >>
        finish
    )
    return dag


configs = get_config()

for config in configs:
    dag_id = f'{config["name"]}_csv'
    globals()[dag_id] = create_pipeline(dag_id, config)
