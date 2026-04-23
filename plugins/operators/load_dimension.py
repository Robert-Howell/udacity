from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 mode="append",  # 'append' or 'truncate-insert'
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.mode = mode

    def execute(self, context):
        self.log.info(f"Starting LoadDimensionOperator for table {self.table} with mode {self.mode}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == 'truncate-insert':
            self.log.info(f"Truncating table {self.table} before insert")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Inserting data into {self.table}")
        redshift.run(self.sql_statement)

        self.log.info(f"LoadDimensionOperator completed for table {self.table}")
