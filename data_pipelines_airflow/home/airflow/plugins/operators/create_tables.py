from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    @apply_defaults
    def __init__(self, redshift_connection_id="", sql_query="", *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        
        #mapping arguments
        self.redshift_conectionn_id = redshift_connection_id
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        
        #run fact  query and log
        self.log.info("executing create table if not exists")
        try:
            redshift.run(self.sql_query)
        except Exception as error: print(error)