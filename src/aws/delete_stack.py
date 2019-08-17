import boto3
import botocore
import configparser
import os
import time


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


def delete_stack():

    """
    Delete the Sparkify stack.
    """

    cloudformation.delete_stack(
        StackName=config['CLOUDFORMATION']['STACK_NAME']
    )


def delete_sparkify_stack():

    """
    Launches the Sparkify stack deletion and waits for
    the defined resources to be also removed.
    """

    # Deletes the stack.
    print('Deleting the stack. This may take awhile, please be patient.')
    delete_stack()

    # Until the resources are removed.
    while True:

        try:
            # Gets the info corresponding the stack.
            description = get_stack_info()
        except botocore.exceptions.ClientError:
            # Boto raises a ClientError if the stack does not exists. This
            # means that the resources have been properly removed.
            print('Resources deleted :-)')
            break
        else:
            # Waits a few seconds until try again.
            print('Current status: {}'.format(description['StackStatus']))
            print('Asking again in {} seconds'.format(delay))
            time.sleep(delay)


if __name__ == '__main__':
    delete_sparkify_stack()
