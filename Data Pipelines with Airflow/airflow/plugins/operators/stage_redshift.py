from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

import logging

# Creating a custom operator StageToRedshiftOperator
# This operator copies data from S3 to Redshift staging tables
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    # SQL query for copying data
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                FORMAT AS JSON '{}'
                REGION 'us-west-2'
    """

    @apply_defaults
    # Define operator parameters
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_prefix = "",
                 json_path = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map parameters
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.json_path = json_path
        

    def execute(self, context):
        # Get AWS credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # Create Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Delete data from table before copying a new batch
        redshift.run(f'DELETE FROM {self.table}')
        self.log.info("Copying data from S3 to Redshift")
        # Create a path S3 path
        s3_path = f'{self.s3_bucket}/{self.s3_prefix}'
        # Create a SQL query (formatted_sql) to load data from S3 to Readshift
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        # Run formatted_sql in Redshift
        redshift.run(formatted_sql)





