from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

DAG_NAME = "task_group_dag"

args = {"owner": "airflow", "start_date": datetime(2021, 11, 11)}


def subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}", default_args=args, catchup=False, schedule_interval=None
    )

    for i in range(5):
        DummyOperator(task_id=f"{child_dag_name}-task-{i + 1}", default_args=args, dag=dag_subdag)

    return dag_subdag


with DAG(dag_id=DAG_NAME, default_args=args, schedule_interval=None, tags=["example"]) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup("inside_section_1") as inside_section_1:
        for i in range(5):
            DummyOperator(task_id=f"task-{i + 1}")

    some_other_task = DummyOperator(task_id="some-other-task")

    section_2 = SubDagOperator(task_id="section-2", subdag=subdag(DAG_NAME, "section-2", args))

    end = DummyOperator(task_id="end")

    start >> inside_section_1 >> some_other_task >> section_2 >> end
