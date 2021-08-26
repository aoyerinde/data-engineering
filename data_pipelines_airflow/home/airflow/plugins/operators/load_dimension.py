from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    #templated insert statement
    insert_sql_template = """insert into {} {};"""

    @apply_defaults
    def __init__(self, redshift_connection_id="", table="", truncate_table=False, insert_query="", *args, **kwargs):
        
        #mapping arguments
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_connection_id = redshift_connection_id
        self.table = table
        self.truncate_table = truncate_table
        self.insert_query = insert_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        
        #truncate table only when set e.g full data refresh
        if self.truncate_table:
            self.log.info("truncate data from destination redshift table")
            try:
                redshift.run("truncate table {}".format(self.table))
            except Exception as error: print(error)

        dimension_insert_query = LoadDimensionOperator.insert_sql_template.format(self.table, self.insert_query)
        
        #run query and log 
        self.log.info("dimension_insert_query run")
        try:
            redshift.run(dimension_insert_query)
        except Exception as error: print(error)
