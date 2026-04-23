from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 test_cases=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        if test_cases is None:
            test_cases = []
        self.test_cases = test_cases

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for i, test in enumerate(self.test_cases):
            sql = test.get('expected_result')
            self.log.info(f'Running test {i+1}: {sql}')
            records = redshift.get_records(sql)

            if not records or len(records) < 1 or records[0] is None:
                raise ValueError(f'Data quality check {i+1} has failed. No results were returned for the query: {sql}')

            actual_result = records[0][0]

            if callable(expected_result):
                if not expected_result(actual_result):
                    raise ValueError(f'Data quality check {i+1} has failed. Query: {sql} returned {actual_result}, which does not meet the expected condition.')
            else:
                if actual_result != expected_result:
                    raise ValueError(f'Data quality check {i+1} has failed. Query: {sql} returned {actual_result}, expected {expected_result}.')
            self.log.info(f'Data quality check {i+1} passed.')
        self.log.info('All data quality checks have passed.')