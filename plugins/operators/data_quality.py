from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 condition_check = [],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.condition_check = condition_check
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('Executing DataQualityOperator: ...')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        for test in self.condition_check:
            table = test.get("table")
            column = test.get("column")
            result_check = test.get('result')
            sql_check = f'SELECT COUNT(*) FROM {table} WHERE {column} IS NULL'

            records = redshift.get_records(sql_check)[0]
            if records[0] == result_check:
                self.log.info("Data quality check passed")
            else:
                self.log.info("Data quality check failed")

