from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=[],
                 aws_credentials_id="",
                 redshift_conn_id="",
                 test_stmt=None,
                 result=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.test_stmt = test_stmt
        self.result = result

    def execute(self, context):
        """
        Perform data quality checks on resulting fact and dimension tables.
        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
        table: string
            table located in redshift cluster
        test_stmt: string
            test SQL command to check validity of target table
        result: string
            result of test_stmt to check validity
        """
        
        aws_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            records = aws_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                self.log.error(f"Data quality check failed. {table} returned no results")

            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
        
        if self.test_stmt:
            output = aws_hook.get_first(self.test_stmt)
            if self.result != output:
                raise ValueError(f"Fail: {output} != {self.result}")

