from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key')
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
    """

    def __init__(self, aws_credentials_id = '', redshift_conn_id = '', table = '', s3_bucket = '', s3_key = '', region = '', json_format = '', *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_format = json_format

    def execute(self, context):
        self.log.info('Executing StageToRedshiftOperator: ...')
        metastoreBackend = MetastoreBackend()
        
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.region,
            self.json_format
        )
        print(formatted_sql)
        redshift_hook.run(formatted_sql)






