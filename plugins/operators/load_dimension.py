from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 truncate= True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        self.log.info('Executing LoadDimensionOperator: ...')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'---Truncate table {self.table}---')
            redshift.run(f'TRUNCATE TABLE {self.table}')
        self.log.info(f'---Loading dimension table {self.table}---')
        redshift.run(f'INSERT INTO {self.table}\n' + self.sql)
        self.log.info(f'Finished loading dimension table {self.table}')