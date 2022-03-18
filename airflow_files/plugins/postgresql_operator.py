from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator


class PostgreSQLCountRows(BaseOperator):
    def __init__(self, table_name: str, postgres_conn_id: str = "postgres_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql_to_count_rows = f"SELECT COUNT(*) FROM {self.table_name};"
        result = hook.get_first(sql=sql_to_count_rows)
        number_of_rows = result[0]
        return number_of_rows
