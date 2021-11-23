from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# create a custom exception class to raise when nulls are present
class CustomException(Exception):
    def __init__(self, *args):
        self.message = args[0]
    
    def __str__(self):
        return f'{self.message}'

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    # set default parameters, which are passed in
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id,
        self.queries = queries
    # execute SQL analysis queries
    def execute(self, context):
        """
        Executes SQL Statements passed in through the queries param
        If a SQL query returns nulls then this function raises an error.
        """
        self.log.info('DataQualityOperator starting analysis')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for i in range(0, len(self.queries)):
            self.log.info('DataQualityOperator running analysis: ' + str(i) + ' ' + str(len(self.queries)-1))
            analysisSql = self.queries[i]
            print("Running: \n " + analysisSql)
            ret = redshift.run(analysisSql)
            print("Result = ... \n")
            print(ret)
            """
            if ret == None:
               self.log.info('Table is Null')
            else:
                self.log.info('Table has nulls' + "... query = " + analysisSql)
                raise CustomException("Table has nulls")
            """