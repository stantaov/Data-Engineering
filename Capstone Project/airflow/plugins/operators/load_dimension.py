from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Creating a custom operator LoadDimensionOperator
# This operator copies data from staging tables to the dimension tables
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define operator parameters
                 redshift_conn_id = '',
                 table = '',
                 sql_statement = '',
                 append_data = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map parameters
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.append_data = append_data
    
    # Create execute function to run SQL queries in Redshift
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id =  self.redshift_conn_id)
        self.log.info('Loading data to dimension tables')
        if self.append_data:
            sql_statement = f"INSERT INTO {self.table} {self.sql_statement}"
            redshift.run(sql_statement)
        else:
            sql_statement = f"DELETE FROM {self.table}"
            redshift.run(sql_statement)
            sql_statement = f"INSERT INTO {self.table} {self.sql_statement}"
            redshift.run(sql_statement)
        
