import logging
import uuid
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from postgresql_operator import PostgreSQLCountRows

config = {
    "users_dag": {"schedule_interval": None, "start_date": datetime(2021, 11, 1), "table_name": "users"},
    "products_dag": {"schedule_interval": "30 * * * *", "start_date": datetime(2021, 10, 1), "table_name": "products"},
    "logs_dag": {"schedule_interval": None, "start_date": datetime(2021, 9, 1), "table_name": "logs"},
}


def create_dag(dag_id, schedule_interval, default_args, table_name):
    def check_table_exists(table_name):
        hook = PostgresHook()

        # get schema name
        sql_to_get_schema = "SELECT * FROM pg_tables;"
        query = hook.get_records(sql=sql_to_get_schema)
        for result in query:
            if "airflow" in result:
                schema = result[0]
                break

        # check table exist
        sql_to_check_table_exist = (
            "SELECT * FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}';"
        )
        query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))

        if query:
            # table exists
            return "insert_row"
        else:
            # table doesn't exist
            return "create_table"

    with DAG(dag_id, schedule_interval=schedule_interval, default_args=default_args) as dag:

        @dag.task(queue="jobs_queue")
        def print_process_start(dag_id: str) -> None:
            logging.info(f"{dag_id} start processing tables in database")

        get_user = BashOperator(task_id="get_current_user", bash_command="whoami", queue="jobs_queue")

        choose_task = BranchPythonOperator(
            task_id="check_table_exists", python_callable=check_table_exists, op_args=[table_name], queue="jobs_queue"
        )

        create_table = PostgresOperator(
            task_id="create_table",
            sql=f"""CREATE TABLE {table_name}(
                custom_id integer NOT NULL,
                user_name VARCHAR (50) NOT NULL,
                timestamp TIMESTAMP NOT NULL);""",
            queue="jobs_queue",
        )

        insert_row = PostgresOperator(
            task_id="insert_row",
            sql=f"INSERT INTO {table_name} "
            + "VALUES (%s, '{{ti.xcom_pull(task_ids='get_current_user', key='return_value')}}', %s);",
            parameters=(uuid.uuid4().int % 123456789, datetime.now()),
            trigger_rule=TriggerRule.NONE_FAILED,
            queue="jobs_queue",
        )

        query_table = PostgreSQLCountRows(task_id="query_table", table_name=table_name, queue="jobs_queue")

        print_process_start(dag_id) >> get_user >> choose_task >> [insert_row, create_table]
        create_table >> insert_row
        insert_row >> query_table

    return dag


for dag_id in config:
    schedule_interval = config[dag_id]["schedule_interval"]

    default_args = {"start_date": config[dag_id]["start_date"]}
    table_name = config[dag_id]["table_name"]

    globals()[dag_id] = create_dag(dag_id, schedule_interval, default_args, table_name)
