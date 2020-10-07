from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 sql_query: str = "",
                 empty_table: bool = False,
                 table_name: str = "",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.empty_table = empty_table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.empty_table:
            self.log.info(f"Empty table {self.table_name}")
            redshift_hook.run(f"DELETE FROM {self.table_name}")

        self.log.info(f"Running to load the dimension Table {self.table_name}")
        redshift_hook.run(self.sql_query)
        self.log.info(f"Dimension Table {self.table_name} ready!")
