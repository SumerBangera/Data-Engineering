from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="redshift",
                 sql_stmt="",
                 append_data=False, 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.append_data = append_data
        
    def execute(self, context):
        """
        Insert data into dimension tables from staging events and song data.
        
        Default option is 'append_data=False' to apply 'truncate-insert' method 
        to empty the target tables prior before loading the data. 
        Users can set 'append_data=True' to overide the default 'truncate-insert'
        setting and use 'append-only' option
        
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
            Default is False ('truncate-insert'). True implies 'append-only' option            .
        """
        
        aws_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
       
        if self.append_data == False:
            aws_hook.run(f"TRUNCATE TABLE {self.table}")
            self.log.info(f"Deleted data from {self.table}")
            
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_stmt})"
        aws_hook.run(formatted_sql)
        self.log.info(f"Success: {self.task_id}")
        data_rows = aws_hook.run(f'SELECT COUNT(*) from {self.table}')
        self.log.info(f"Data in table {self.table}: {data_rows}")
        