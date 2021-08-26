from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
     #templated copy statement
    copy_sql = """ COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' {};"""
    @apply_defaults
    def __init__(self, redshift_connection_id="", aws_credentials_id="", table="",
                 s3_bucket="", s3_key="", extra_params="", *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        #mapping arguments
        self.redshift_connection_id = redshift_connection_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.extra_params = extra_params

    def execute(self, context):
        #set credentials for aws using hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        
        
        #run truncate  and statement and log 
        self.log.info("truncate destination redshift table")
        redshift.run("truncate table {}".format(self.table))
        
        #form s3 path with permissions, and copy query
        self.log.info("copy from s3 to redshift")
        s3_parsed_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, s3_parsed_key)
        staging_table_copy = StageToRedshiftOperator.copy_sql.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.extra_params)
        
        #logging
        self.log.info("staging_table_copy run")
        #run copy statement, catch and print errors
        try:
            redshift.run(staging_table_copy)
        except Exception as error: print(error)





