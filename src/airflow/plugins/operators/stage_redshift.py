from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id=None,
        iam_role_arn=None,
        s3_prefix=None,
        target_table=None,
        json_path='auto',
        *args,
        **kwargs
    ):

        """
        Initializes a new instance of the class StageToRedshiftOperator.

        Parameters:
            redshift_conn_id (str): The Redshift connection identifier.
            iam_role_arn (str):The IAM role ARN that will be used
                from Redshift to execute the COPY query. This role
                must have permissions to read the source S3 bucket.
            s3_prefix (str): The S3 prefix where the source data is stored.
            target_table (str): The name of the table where the records
                will be inserted.
            json_path (str): The path to the JSON file that contains the
                links to the individual files from the source data that
                must be copied.
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id
        self._iam_role_arn = iam_role_arn
        self._s3_prefix = s3_prefix
        self._target_table = target_table
        self._json_path = json_path

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

        # Checks if the IAM role ARN is valid.
        if self._iam_role_arn is None \
                or not isinstance(self._iam_role_arn, str) \
                or self._iam_role_arn.strip() == '':
            raise ValueError('The IAM role ARN cannot be null or empty.')

        # Checks if the S3 prefix is valid.
        if self._s3_prefix is None \
                or not isinstance(self._s3_prefix, str) \
                or self._s3_prefix.strip() == '':
            raise ValueError('The S3 prefix cannot be null or empty.')

        # Checks if the target table is valid.
        if self._target_table is None \
                or not isinstance(self._target_table, str) \
                or self._target_table.strip() == '':
            raise ValueError('The target table cannot be null or empty.')

    def execute(self, context):

        """
        Copies the source data from S3 to a given stage table in Redshift.

        Parameters:
            context (dict): Contains info related to the task instance.
        """

        # Validates the operator parameteres.
        self.check_invalid_params()

        # Builds the query.
        query = SqlQueries.staging_table_copy.strip().format(
            self._target_table,
            self._s3_prefix,
            self._iam_role_arn,
            self._json_path
        )

        # Logs and executes the query.
        self.log.info(query)
        PostgresHook(postgres_conn_id=self._redshift_conn_id).run(query)
