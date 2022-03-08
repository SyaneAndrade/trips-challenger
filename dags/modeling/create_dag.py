from airflow import DAG
import airflow
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from utils.config import Config


def get_config():
    config = Config("/opt/airflow/config/modeling_config.yaml")
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
    return SparkSubmitOperator(task_id=f'spark_submit_{config["name"]}', 
                            application=config["application"],
                            name=config["name"],
                            conn_id=config["conn"],
                            application_args=[
                                              config["name"],
                                             config["spark_master"]],
                            jars="/usr/local/spark/resources/jars/postgresql-42.3.3.jar",
                            dag=dag)

def create_pipeline(dag_id, configs):
    dag = create_dag(dag_id)
    start = create_start(dag)
    finish = create_finish(dag)

    for config in configs:
        spark_submit = create_spark_submit(dag, config)
        (
            start >>
            spark_submit >>
            finish
        )
        
    return dag


configs = get_config()

dag_id = 'trips_modeling'
globals()[dag_id] = create_pipeline(dag_id, configs)
