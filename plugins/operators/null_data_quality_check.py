from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# create a custom exception class to raise when nulls are present
class CustomException(Exception):
    def __init__(self, *args):
        self.message = args[0]
    
    def __str__(self):
        return f'{self.message}'

class NullDataQualityOperator(BaseOperator):

    query = """
        SELECT {} FROM {}
        WHERE {} IS NULL ;
        """

    ui_color = '#89DA59'
    
    # set default parameters, which are passed in
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table='',
                 values=[],
                 *args, **kwargs):

        super(NullDataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id,
        self.table = table,
        self.values = values

    # execute SQL analysis queries
    def execute(self, context):
        """
        Executes SQL Statements passed in through the queries param
        If a SQL query returns nulls then this function raises an error.
        """
        self.log.info('DataQualityOperator starting analysis')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if type(self.table) is tuple:
            self.table = self.table[0]
            print("table = :\n" + str(self.table))
        else:
            print("table = :\n" + str(self.table))

        print("values= :\n" + str(self.values))
        for key in self.values:
            analysisSql = self.query.format(key, self.table, key)
            self.log.info('DataQualityOperator running analysis: ' + analysisSql)
            ret = redshift.get_first(analysisSql)
            if ret == None:
                self.log.info('Table and key pair is Valid\n table = ' + self.table + '\n key = ' + key)
            else:
                self.log.info(self.table + ' has null primary key: ' + key + "\n ... query = " + analysisSql)
                raise CustomException("Table has nulls")