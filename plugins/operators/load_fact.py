from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 query='',
                 table="",
                 mode="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.table = table
        self.mode = mode

    def execute(self, context):
        """
        Executes SQL Statement passed in through the query
        This function should be used to create a fact table from a table stored
        on the RDS
        """
        self.log.info('LoadFactOperator starting insert')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == "delete":
            self.log.info(f"Clearing data from {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))
        factSql = self.query
        redshift.run(factSql)
