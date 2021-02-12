from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                REGION '{}'
                {}'auto'
                {}
                {}
                TIMEFORMAT as 'epochmillisecs'
                TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            """

    @apply_defaults
    def __init__(self,
                 table="staging_temp_table",
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 s3_bucket="yelp-data",
                 s3_key="", #"temp_key",
                 region="ap-southeast-1",
                 file_format="JSON",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.execution_date = kwargs.get('execution_date')
        
        
    def execute(self, context):
        """
        Copy data from S3 buckets to Redshift cluster into staging tables.
        
        Parameters:
        -----------
        table: string
            target table name in the Redshift cluster
        redshift_conn_id: string
            Airflow connection to Redshift cluster
        aws_credentials_id: string
            credentials of Airflow connection to AWS
        s3_bucket: string
            source bucket with data for staging
        s3_key: string
            path with data for staging
        file_format: string
            file format to copy data, options - JSON (default), CSV
       """
        
        self.log.info(f"Starting {self.table} StageToRedshiftOperator")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Deleting existing data from {self.table} in Redshift")
        redshift.run("DELETE FROM {}".format(self.table))
        
        

        s3_path = "s3://{}/{}.{}".format(self.s3_bucket, self.s3_key, self.file_format)
        self.log.info(f"Copying data from S3 bucket: {s3_path} into Redshift table: {self.table}")
        
        
        # self.log.info(f"Copying {self.table} from S3 to Redshift")
        # s3_path = "s3://{}".format(self.s3_bucket)
        
        # if self.execution_date:
        #     # Backfill a specific date
        #     year = self.execution_date.strftime("%Y")
        #     month = self.execution_date.strftime("%m")
        #     day = self.execution_date.strftime("%d")
        #     s3_path = '/'.join([s3_path, str(year), str(month), str(day)])
        # s3_path = s3_path + '/' + self.s3_key             
        
        if self.file_format == 'CSV':
            delimiter = "DELIMITER ','"
            header = "IGNOREHEADER 1"
        else:
            delimiter = ""
            header = ""
                     
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                                                                self.table,
                                                                s3_path,
                                                                credentials.access_key,
                                                                credentials.secret_key,
                                                                self.region,
                                                                self.file_format,
                                                                delimiter,
                                                                header
                                                            )
        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")
       
