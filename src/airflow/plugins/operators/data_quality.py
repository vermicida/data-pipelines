from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    tables = (
        'artists',
        'users',
        'songs',
        'time',
        'songplays'
    )

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id=None,
        tables=None,
        *args,
        **kwargs
    ):
        """
        Initializes a new instance of the class DataQualityOperator.

        Parameters:
            redshift_conn_id (str): The Redshift connection identifier.
            tables (iterable): A tuple with the name of those tables
                which data must be validated.
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id
        self._tables = tables

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

        # Checks if the tables tuple is valid.
        if self._tables is None \
                or not isinstance(self._tables, tuple) \
                or len(self._tables) == 0:
            raise ValueError('The tables tuple cannot be null or empty.')

        for table in self._tables:
            if not isinstance(table, str) \
                    or table.strip() == '' \
                    or table not in self.tables:
                message = 'Available values for the tables tuple: {}'
                raise ValueError(message.format(', '.join(self.tables)))

    def execute(self, context):

        """
        Validates that the given tables meet the data quality threshold.

        Parameters:
            context (dict): Contains info related to the task instance.
        """

        # Validates the operator parameteres.
        self.check_invalid_params()

        postgres = PostgresHook(postgres_conn_id=self._redshift_conn_id)

        for table in self._tables:

            query = 'SELECT COUNT(*) FROM {}'.format(table)

            self.log.info(query)

            records = postgres.get_records(query)

            if len(records) == 0 or len(records[0]) == 0 or records[0][0] == 0:
                message = 'The table {} has not passed the data quality check.'.format(table)
                self.log.error(message)
                raise ValueError(message)

            message = 'The table {} has passed the data quality check with {} records.'
            self.log.info(message.format(table, records[0][0]))
