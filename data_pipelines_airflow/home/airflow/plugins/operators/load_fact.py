from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
     #templated insert statement
    insert_sql_template  = """insert into {} {}; """
    @apply_defaults
    def __init__(self, redshift_connection_id="", table="", sql_query="", *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        #mapping arguments
        self.redshift_connection_id = redshift_connection_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)


        fact_insert_query = LoadFactOperator.insert_sql_template .format(self.table, self.sql_query)
        
        #run fact insert query and log
        self.log.info("fact_insert_query run")
        try:
            redshift.run(fact_insert_query)
        except Exception as error: print(error)
