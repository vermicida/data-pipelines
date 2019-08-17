from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    dimensions = {
        'artists': 'artist_id',
        'users': 'userid',
        'songs': 'song_id',
        'time': 'start_time'
    }

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id=None,
        target_table=None,
        dimension=None,
        truncate=True,
        pk_field=None,
        *args,
        **kwargs
    ):

        """
        Initializes a new instance of the class LoadDimensionOperator.

        Parameters:
            redshift_conn_id (str): The Redshift connection identifier.
            target_table (str): The name of the table where the records
                will be inserted.
            dimension (str): The dimension data you want to extract from the
                staging tables. The available options are: 'songs', 'artists',
                'users' and 'time'.
            truncate (bool): When True, the target table will be truncated
                before the dimension data is inserted. When False, the
                dimension data is appended instead.
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id
        self._target_table = target_table
        self._dimension = dimension
        self._truncate = truncate
        self._pk_field = pk_field

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

        # Checks if the dimension is valid.
        if self._dimension is None \
                or not isinstance(self._dimension, str) \
                or self._dimension.strip() == '':
            raise ValueError('The dimension cannot be null or empty.')

        dimensions = list(self.dimensions.keys())
        if self._dimension not in dimensions:
            message = 'Available values for the dimension: {}'
            raise ValueError(message.format(', '.join(dimensions)))

        # Checks if the truncate flag is valid.
        if self._truncate is None \
                or not isinstance(self._truncate, bool):
            raise ValueError('The truncate flag must be boolean.')

        # Checks if the PK field is valid.
        if not self._truncate \
                and (
                    self._pk_field is None
                    or not isinstance(self._pk_field, str)
                    or self._pk_field.strip() == ''
                ):
            raise ValueError('The PK field cannot be null or empty when truncate is False.')

    def execute(self, context):

        """
        Extracts a given dimension data the from stage tables and writes
        it to a target table.

        Parameters:
            context (dict): Contains info related to the task instance.
        """

        # Validates the operator parameteres.
        self.check_invalid_params()

        # Gets the select query corresponding the given dimension.
        select_query = getattr(
            SqlQueries,
            '{}_table_insert'.format(self._dimension)
        ).strip()

        # Builds the query.
        if self._truncate:

            # If the truncate flag is True, we must truncate the target
            # table first, and then do the UPSERT.
            query = """
                TRUNCATE TABLE {target_table};
                INSERT INTO {target_table}
                {select_query};
            """.format(
                target_table=self._target_table,
                select_query=select_query
            )

        else:

            # If the truncate flag is False, we must do the UPSERT handling
            # those records that already exists.
            query = """
                INSERT INTO {target_table}
                {select_query}
                {clause} NOT EXISTS (
                    SELECT {pk_field}
                    FROM {target_table}
                    WHERE src.{src_pk_field} = {target_table}.{pk_field}
                );
            """.format(
                clause='AND' if 'WHERE' in select_query else 'WHERE',
                target_table=self._target_table,
                src_pk_field=self.dimensions[self._dimension],
                pk_field=self._pk_field,
                select_query=select_query
            )

        # Logs and executes the query.
        self.log.info(query)
        PostgresHook(postgres_conn_id=self._redshift_conn_id).run(query)
