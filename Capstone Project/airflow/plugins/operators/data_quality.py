from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Creating a custom operator DataQualityOperator
# This operator conducts a simple data quality check
# Checks if tables have any records 
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operator parameters
                 redshift_conn_id = '',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map parameters
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
    
    # Create execute function to run data quality check
    def execute(self, context):
        self.log.info('Data quality check.')
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f'Checking table: {self.tables}.')
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. The is not data in {table}.")
            else: 
                self.log.info(f'Data quality check passed for: {table}. Number of records: {records}')