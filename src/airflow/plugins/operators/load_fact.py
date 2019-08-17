from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id=None,
        target_table=None,
        *args,
        **kwargs
    ):

        """
        Initializes a new instance of the class LoadFactOperator.

        Parameters:
            redshift_conn_id (str): The Redshift connection identifier.
            target_table (str): The name of the table where the records
                will be inserted.
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id
        self._target_table = target_table

    def check_invalid_params(self):

        """
        Checks if the mandatory operator parameters are properly defined.

        Raises:
            ValueError: if any of the parameters is null or empty.
        """

        # Checks if the Redshift connection identifier is valid.
        if self._redshift_conn_id is None \
                or not isinstance(self._redshift_conn_id, str) \
                or self._redshift_conn_id.strip() == '':
            raise ValueError('The Redshift connection identifier cannot be null or empty.')

        # Checks if the target table is valid.
        if self._target_table is None \
                or not isinstance(self._target_table, str) \
                or self._target_table.strip() == '':
            raise ValueError('The target table cannot be null or empty.')

    def execute(self, context):

        """
        Extracts a given fact data from the stage tables and writes it
        to a target table.

        Parameters:
            context (dict): Contains info related to the task instance.
        """

        # Validates the operator parameteres.
        self.check_invalid_params()

        # Builds the query.
        query = 'INSERT INTO {} {}'.format(
            self._target_table,
            SqlQueries.songplays_table_insert.strip()
        )

        # Logs and executes the query.
        self.log.info(query)
        PostgresHook(postgres_conn_id=self._redshift_conn_id).run(query)
