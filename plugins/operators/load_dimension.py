from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load Dimension Operator
    -----------------------
    Operator that loads data into dimension tables
    """
    ui_color = '#80BD9E'
    sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):
        '''
        Dimension table loader

        Arguments:
        ----------
        redshift_conn_id - id for redshift connection\n
        table - db table into which data will be inserted \n\
        sql_query - query to run to insert data \n\
        '''
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading dimension table {}".format(self.table))
        redshift.run(
            self.sql.format(self.table, self.sql_query)
        )

