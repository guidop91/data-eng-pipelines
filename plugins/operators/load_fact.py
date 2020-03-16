from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Load Fact Operator
    -----------------------
    Operator that loads data into fact tables
    """    
    ui_color = '#F98866'
    insert_sql = """
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
        Fact table loader

        Arguments:
        ----------
        redshift_conn_id - id for redshift connection\n
        table - db table into which data will be inserted \n\
        sql_query - query to run to insert data \n\
        '''
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading fact table {}".format(self.table))
        formatted_sql = LoadFactOperator.insert_sql.format(self.table, self.sql_query)
        redshift.run(formatted_sql)
