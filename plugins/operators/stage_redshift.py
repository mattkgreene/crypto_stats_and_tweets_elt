from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    {} ;
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 extra="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        # self.delimiter = delimiter
        self.extra = extra
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        """
        Executes SQL Copy statement, which is defined at the top of the class.
        This function is specifically used to copy data from S3 Buckets to
        an AWS Redshift Cluster DW
        """
        self.log.info('StageToRedshiftOperator starting')
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        self.log.info("Getting Credentials from aws hook")
        credentials = aws_hook.get_credentials()
        self.log.info("Connecting to Postgre Hook with redshift conn id")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.extra
        )
        redshift.run(formatted_sql)
        self.log.info("Copy s3 to redshift finished")





