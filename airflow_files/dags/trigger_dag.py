from datetime import datetime

from airflow import DAG
from airflow.hooks.filesystem import FSHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from slack_operator import SlackNotification
from smart_file_sensor import SmartFileSensor

DAG_ID_TO_TRIGGER = "logs_dag"
SCHEDULE_INTERVAL = None
default_args = {"start_date": datetime(2021, 1, 1)}


def process_results_subdag(parent_dag_name: str, child_dag_name: str, args: dict) -> DAG:
    def print_result(ti, **kwargs) -> None:
        # Note: We can't do it anymore, because we changed PythonOperator
        # to a custom PostgreSQLCountRows operator in jobs_dag.py
        # msg = ti.xcom_pull(task_ids="query_table", dag_id="logs_dag", key="the_message")
        # print(msg)
        print(kwargs)

    dag_subdag = DAG(
        dag_id="{0}.{1}".format(parent_dag_name, child_dag_name),
        schedule_interval=SCHEDULE_INTERVAL,
        default_args=args,
    )

    with dag_subdag:
        sensor_dag = ExternalTaskSensor(
            task_id="sensor_triggered_dag",
            external_dag_id=DAG_ID_TO_TRIGGER,
            external_task_id=None,
            allowed_states=["success"],
        )

        print_result_op = PythonOperator(task_id="print_result", python_callable=print_result)

        path = FSHook(conn_id="fs_default").get_path()
        file_name = Variable.get("name_path_variable", default_var="run")

        remove_run_file = BashOperator(task_id="remove_run_file", bash_command=f"rm {path}/{file_name}")

        create_timestamp = BashOperator(
            task_id="create_finished_timestamp", bash_command=f"touch {path}/" + "finished_#{{ ts_nodash }}"
        )

        sensor_dag >> print_result_op >> remove_run_file >> create_timestamp

    return dag_subdag


with DAG(dag_id="trigger_dag", schedule_interval=SCHEDULE_INTERVAL, default_args=default_args) as dag:
    file_name = Variable.get("name_path_variable", default_var="run")

    wait_file = SmartFileSensor(task_id="wait_run_file", poke_interval=30, filepath=file_name, fs_conn_id="fs_default")

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_dag", trigger_dag_id=DAG_ID_TO_TRIGGER, execution_date="{{ execution_date }}"
    )

    process_results = SubDagOperator(
        task_id="process_results",
        subdag=process_results_subdag(
            parent_dag_name="trigger_dag", child_dag_name="process_results", args=default_args
        ),
        default_args=default_args,
    )

    alert_slack = SlackNotification(task_id="alert_slack", text="Hello from your app! :tada:")

    wait_file >> trigger_dag >> process_results >> alert_slack
