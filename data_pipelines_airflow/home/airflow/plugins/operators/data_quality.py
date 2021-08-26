from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    @apply_defaults
    def __init__(self, redshift_connection_id="", table="", min_expected_result="",  *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        #mapping arguments
        self.table = table
        self.redshift_connection_id = redshift_connection_id
        self.min_expected_result = min_expected_result
 
    def execute(self, context):
        table = kwargs["params"]["table"]
        redshift_hook = PostgresHook("redshift")
        
        #check dynamically tables for records
        for table_name in table:
            result = redshift_hook.get_records("select count(*) cnt from {}".format(self.table))
            count_num = result[0][0]


            if len(result) < self.expected_result or len(result[0]) < self.expected_result:
                raise ValueError("quality check failed. table {} returned no results".format(self.table))
            elif count_num < self.expected_result:
                raise ValueError("quality check failed. {} contained 0 rows".format(self.table))
                logging.info(" table {} has {} records".format(self.table, count_num))