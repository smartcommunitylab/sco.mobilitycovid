import io
import os
import sys
import time

import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels

from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
load_dotenv()

#
# Batch credentials
#
BATCH_ACCOUNT_NAME = os.getenv("BATCH_ACCOUNT_NAME")
BATCH_ACCOUNT_KEY = os.getenv("BATCH_ACCOUNT_KEY")
BATCH_ACCOUNT_URL = os.getenv("BATCH_ACCOUNT_URL")

#
# Azure Storage
#
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
INPUT_CONTAINER_NAME = 'cuebiq-data'
OUTPUT_CONTAINER_NAME = 'cuebiq-data'

#
# Azure SAS token
#
SAS_TOKEN = os.getenv("SAS_TOKEN")

OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID")
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")
OAUTH_LOGIN = os.getenv("OAUTH_LOGIN")

#
# Script name
#
SCRIPT = "stop_user_clusters-v3.py"
SCRIPT_URL = "https://gist.githubusercontent.com/matteo-s/7b327ceb45f597b0e6c220aeb552da7c/raw/18812bfa98498822f0c7a78df1be3a1de915f10c/stop_user_clusters-v3.py"
VERSION = 3

INIT_CMD = [
    "apt-get update",
    "apt -y install curl",
    "apt-get -y install python3-pip",
    "pip3 install azure-storage-blob",
    "pip3 install pyspark",
    "pip3 install pygeohash",
    "pip3 install numpy",
    "pip3 install pandas",
    "pip3 install pyarrow",
    "pip3 install scikit-learn",
    "apt -y install openjdk-8-jre-headless",
    f"curl {SCRIPT_URL} -o /mnt/batch/tasks/shared/{SCRIPT}"
]

#
# Pool settings
#
POOL_ID = 'StopUserClusters'
DEDICATED_POOL_NODE_COUNT = 0
LOW_PRIORITY_POOL_NODE_COUNT = 1
POOL_VM_SIZE = "Standard_F72s_v2"
POOL_VM_CORE = "72"
POOL_VM_RAM = "136g"
SHUFFLE_PARTITIONS = 1024

#
# Country must be "US" or "IT"
#

COUNTRY = "US"

JOB_ID = "{}_v{}_{}-TX".format(POOL_ID, VERSION, COUNTRY)

# US_STATES = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS',
#              'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY']

US_STATES = ['TX']

# def enumerate_prefixes(start=0, end=256):
#     for i in range(start, end):
#         yield '{:02x}'.format(i)

#
# batch helpers
#


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')


def wait_for_tasks_to_complete(batch_service_client, job_id):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """

    print("Monitoring all tasks for 'Completed' state, you will see many dots")
    while 1:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)


def wrap_commands_in_shell(ostype, commands):
    """Wrap commands in a shell
    :param list commands: list of commands to wrap
    :param str ostype: OS type, linux or windows
    :rtype: str
    :return: a shell wrapping commands
    """
    if ostype.lower() == 'linux':
        return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(
            ';'.join(commands))
    elif ostype.lower() == 'windows':
        return 'cmd.exe /c "{}"'.format('&'.join(commands))
    else:
        raise ValueError('unknown ostype: {}'.format(ostype))


#
# batch handlers
#

def create_pool(batch_service_client, pool_id):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sky
    """
    print('Creating pool [{}]...'.format(pool_id))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/

    # The start task installs libs on each node from an available repository, using
    # an administrator user identity.
    #

    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="Canonical",
                offer="UbuntuServer",
                sku="18.04-LTS",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 18.04"),
        vm_size=POOL_VM_SIZE,
        target_dedicated_nodes=DEDICATED_POOL_NODE_COUNT,
        target_low_priority_nodes=LOW_PRIORITY_POOL_NODE_COUNT,
        start_task=batchmodels.StartTask(
            command_line=wrap_commands_in_shell('linux', INIT_CMD),
            wait_for_success=True,
            user_identity=batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    scope=batchmodels.AutoUserScope.pool,
                    elevation_level=batchmodels.ElevationLevel.admin)),
        )
    )

    batch_service_client.pool.add(new_pool)


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = batch.models.JobAddParameter(
        id=job_id,
        pool_info=batch.models.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)

#
# Main
#


if __name__ == '__main__':

    start_time = datetime.now().replace(microsecond=0)
    print('Batch start: {}'.format(start_time))

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(BATCH_ACCOUNT_NAME,
                                                  BATCH_ACCOUNT_KEY)

    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=BATCH_ACCOUNT_URL)

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        #create_pool(batch_client, POOL_ID)

        # Create the job that will run the tasks.
        create_job(batch_client, JOB_ID, POOL_ID)
        print()
    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    script_file = f"/mnt/batch/tasks/shared/{SCRIPT}"

    for cur_state in US_STATES:
        print(f"add task for {cur_state} ...")

        try:

            # Add the tasks to the job.
            tasks = list()

            tid = "task_{}".format(cur_state)

            commands = "/bin/bash -c \"python3 {} --vm-core {} --vm-ram {} --shuffle-partitions {} --storage '{}' --sas '{}' --oauth-login '{}' --oauth-client-id '{}' --oauth-client-secret '{}' --container-in '{}' --container-out '{}'  --country '{}' --state '{}' \"".format(
                script_file,
                POOL_VM_CORE, POOL_VM_RAM, SHUFFLE_PARTITIONS,
                STORAGE_ACCOUNT_NAME, SAS_TOKEN,
                OAUTH_LOGIN,
                OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET,
                INPUT_CONTAINER_NAME, OUTPUT_CONTAINER_NAME,
                COUNTRY,
                cur_state)

            tasks.append(
                batch.models.TaskAddParameter(
                    id=tid,
                    command_line=commands,
                )
            )

            batch_client.task.add_collection(JOB_ID, tasks)

            print("{} tasks submitted for {}".format(len(tasks), cur_state))

        except batchmodels.BatchErrorException as err:
            print_batch_exception(err)
            raise

    # Pause execution until tasks reach Completed state.
    #wait_for_tasks_to_complete(batch_client, JOB_ID)

    # Print out some timing info
    end_time = datetime.now().replace(microsecond=0)
    print()
    print('Batch end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    # cleanup
    # batch_client.job.delete(JOB_ID)
    # batch_client.pool.delete(POOL_ID)
    print("Done.")
