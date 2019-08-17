import boto3
import configparser
import psycopg2
import os
import time
from queries import SparkifyQueries

# Loads the Sparkify configuration.
config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), 'sparkify.cfg'))

# The CloudFormation client.
cloudformation = boto3.client(
    'cloudformation',
    region_name=config['AWS']['REGION'],
    aws_access_key_id=config['AWS']['ACCESS_KEY_ID'],
    aws_secret_access_key=config['AWS']['SECRET_ACCESS_KEY']
)

# The delay between stack status checks.
delay = 30

# A reference to the builtin function 'print()'.
builtin_print = print


def print(text):

    """
    Prints a timestamp next to the the given text.

    Args:
        text (str): The text to print.
    """

    return builtin_print('{} | {}'.format(
        time.strftime('%H:%M:%S', time.gmtime()),
        text
    ))


def get_output_value(description, key):

    """
    Gets an output value of a given stack description.

    Args:
        description (dict): The stack description object.
        key (str): The key of the output.

    Returns:
        (str): The value of the output.
    """

    outputs = [o for o in description['Outputs'] if o['OutputKey'] == key]
    return None if len(outputs) != 1 else outputs[0]['OutputValue']


def get_stack_info():

    """
    Gets the description of the Sparkify stack.

    Returns:
        (dict): The description of the stack.
    """

    response = cloudformation.describe_stacks(
        StackName=config['CLOUDFORMATION']['STACK_NAME']
    )
    return response['Stacks'][0]


def create_stack():

    """
    Creates the Sparkify stack.

    Returns:
        (str): The identifier of the stack.
    """

    with open(os.path.join(os.getcwd(), 'sparkify_stack.json'), 'r') as f:
        content = f.read()

    cloudformation.create_stack(
        StackName=config['CLOUDFORMATION']['STACK_NAME'],
        TemplateBody=content,
        Capabilities=['CAPABILITY_NAMED_IAM'],
        Parameters=[
            {
                'ParameterKey': 'RoleNameParam',
                'ParameterValue': config['IAM']['ROLE_NAME']
            },
            {
                'ParameterKey': 'ClusterIdentifierParam',
                'ParameterValue': config['REDSHIFT']['CLUSTER_IDENTIFIER']
            },
            {
                'ParameterKey': 'ClusterTypeParam',
                'ParameterValue': config['REDSHIFT']['CLUSTER_TYPE']
            },
            {
                'ParameterKey': 'NodeTypeParam',
                'ParameterValue': config['REDSHIFT']['NODE_TYPE']
            },
            {
                'ParameterKey': 'DBNameParam',
                'ParameterValue': config['REDSHIFT']['DB_NAME']
            },
            {
                'ParameterKey': 'PortParam',
                'ParameterValue': config['REDSHIFT']['PORT']
            },
            {
                'ParameterKey': 'MasterUsernameParam',
                'ParameterValue': config['REDSHIFT']['MASTER_USERNAME']
            },
            {
                'ParameterKey': 'MasterUserPasswordParam',
                'ParameterValue': config['REDSHIFT']['MASTER_USER_PASSWORD']
            }
        ]
    )


def create_tables(cluster_endpoint):

    """
    Creates the staging, dimension and fact tables in the Redshift cluster.

    Parameters:
        cluster_endpoint (str): The Redshift cluster endpoint.
    """

    dsn = 'host={} port={} dbname={} user={} password={}'.format(
        cluster_endpoint,
        config['REDSHIFT']['PORT'],
        config['REDSHIFT']['DB_NAME'],
        config['REDSHIFT']['MASTER_USERNAME'],
        config['REDSHIFT']['MASTER_USER_PASSWORD']
    )

    with psycopg2.connect(dsn) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            print('Creating table: staging_events')
            cur.execute(SparkifyQueries.staging_events_table_create)
            print('Creating table: staging_songs')
            cur.execute(SparkifyQueries.staging_songs_table_create)
            print('Creating table: time')
            cur.execute(SparkifyQueries.time_table_create)
            print('Creating table: users')
            cur.execute(SparkifyQueries.users_table_create)
            print('Creating table: artists')
            cur.execute(SparkifyQueries.artists_table_create)
            print('Creating table: songs')
            cur.execute(SparkifyQueries.songs_table_create)
            print('Creating table: songplays')
            cur.execute(SparkifyQueries.songplays_table_create)


def create_sparkify_stack():

    """
    Launches the Sparkify stack creation and waits for the defined
    resources to be also created and ready to use.
    """

    # Creates the stack.
    print('Creating the stack. This may take awhile, please be patient.')
    create_stack()

    # Until the resources are provisioned.
    while True:

        # Gets the info corresponding the stack.
        description = get_stack_info()
        print('Current status: {}'.format(description['StackStatus']))

        # If the resources are provisioned.
        if description['StackStatus'] == 'CREATE_COMPLETE':

            # Gets some outputs from the stack.
            role_arn = get_output_value(description, 'SparkifyRoleArn')
            cluster_endpoint = get_output_value(description, 'SparkifyClusterEndpoint')

            # Creates the tables in the Redshift cluster.
            create_tables(cluster_endpoint)

            # Prints the role ARN and the cluster endpoint.
            print('\n'.join((
                'Resources created!',
                '',
                '1) Create a new Airflow connection with the following values:',
                '',
                '  Conn Id:   redshift',
                '  Conn Type: Postgres',
                '  Host:      {}',
                '  Schema:    {}',
                '  Login:     {}',
                '  Password:  {}',
                '  Port:      {}',
                '',
                '2) Update the Airflow variable \'sparkify_config\' with this:',
                '',
                '  {{',
                '    "iam": {{',
                '      "role_arn": "{}"',
                '    }}',
                '  }}',
                ''
            )).format(
                cluster_endpoint,
                config['REDSHIFT']['DB_NAME'],
                config['REDSHIFT']['MASTER_USERNAME'],
                config['REDSHIFT']['MASTER_USER_PASSWORD'],
                config['REDSHIFT']['PORT'],
                role_arn
            ))
            break

        # Waits a few seconds until try again.
        print('Not ready yet! Asking again in {} seconds'.format(delay))
        time.sleep(delay)


if __name__ == '__main__':
    create_sparkify_stack()
