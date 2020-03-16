from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Data Quality Operator
    ---------------------
    Checks tables to make sure they have at least one record in them.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):
        '''
        Data Quality Operator Constructor

        Arguments:
        ----------
        redshift_conn_id - id for redshift connection\n
        tables - list of db tables to quality check \n\
        '''
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            self.log.info(f"Checking Data Quality for {table} table")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records[0]) < 1 or len(records) < 1:
                raise ValueError(f"Data quality check failed: Table {table} has no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed: Table {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
