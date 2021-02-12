from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="redshift",
                 sql_stmt="",
                 append_data=True, 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.append_data = append_data

    def execute(self, context):
        """
        Insert data into fact tables from staging events and song data.
        
        Fact tables are generally significantly large and thus append only
        methods should be utilized.Hence, the default option is 'append-only'.
        Users can set 'append_data=False' to overide the default 'append-only'
        setting and use 'delete-load' option
        
        Parameters:
        -----------
        table: string
            target table name in the Redshift cluster
        redshift_conn_id: string
            Airflow connection to Redshift cluster
        sql_stmt: string
            SQL query to insert data into the table
        append_data: boolean
            determines whether data should be appended to existing data or delete all and insert into the tables.
            Default is True (append-only). False implies 'delete-load' option            .
        """
        
        aws_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if self.append_data == False:
            aws_hook.run(f"TRUNCATE TABLE {self.table}")
            self.log.info(f"Deleted data from {self.table}")
            
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_stmt})"
        aws_hook.run(formatted_sql)
        self.log.info(f"Success: {self.task_id}")
        
