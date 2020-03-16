from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Stage data from S3 to Redshift Operator
    -----------------------
    Operator that loads data from S3 to Redshift
    """    
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        {}
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        json_path="",
        ignore_headers=1,
        *args, **kwargs
    ):
        '''
        Fact table loader

        Arguments:
        ----------
        redshift_conn_id - id for redshift connection\n
        table - db table into which data will be inserted \n\
        s3_bucket - bucket in s3 from which to read data \n\
        s3_key - key in bucket from which to read data \n\
        aws_credentials_id - credentials from aws to use to read data \n\
        ignore_headers - whether to ignore headers in data reading \n\
        json_path - additional information about table in json format \n\
        '''
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.ignore_headers=ignore_headers
        self.json_path=json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        self.log.info('Rendered key is ' + rendered_key)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            "JSON '{}'".format(self.json_path)
        )
        redshift.run(formatted_sql)





