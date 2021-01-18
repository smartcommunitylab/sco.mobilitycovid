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

#
# Script name
#
SCRIPT = "compute_bin_hours.py"
SCRIPT_URL = "https://gist.githubusercontent.com/matteo-s/7b327ceb45f597b0e6c220aeb552da7c/raw/8c5435bad153232ebaedf748f810994647bd5438/compute_bin_hours.py"
VERSION = 2

INIT_CMD = [
    "apt-get update",
    "apt -y install curl",
    "apt-get -y install python3-pip",
    "pip3 install azure-storage-blob",
    "pip3 install pyspark",
    "pip3 install pygeohash",
    "pip3 install numpy",
    "pip3 install pandas",
    "apt -y install openjdk-8-jre-headless",
    f"curl {SCRIPT_URL} -o /mnt/batch/tasks/shared/{SCRIPT}"
]

#
# Pool settings
#
POOL_ID = 'ComputeBinHours'
DEDICATED_POOL_NODE_COUNT = 0
LOW_PRIORITY_POOL_NODE_COUNT = 1
POOL_VM_SIZE = "Standard_E8_v3"
POOL_VM_CORE = "8"
POOL_VM_RAM = "58g"
SHUFFLE_PARTITIONS = 512

#
# Dates and countries (comprehensive of the end day), nat must be "US" or "IT"
#

START_DATE = datetime.strptime(
    "2020-07-01", "%Y-%m-%d").replace(tzinfo=timezone.utc)
END_DATE = datetime.strptime(
    "2020-07-31", "%Y-%m-%d").replace(tzinfo=timezone.utc)
COUNTRY = "US"

JOB_ID = "{}_v{}_{}-{}-{}".format(POOL_ID, VERSION, COUNTRY,
                              START_DATE.strftime("%Y%m%d"), END_DATE.strftime("%Y%m%d"))


def daterange(start_date, end_date, interval=timedelta(days=1)):
    cur_date = start_date
    while(cur_date <= end_date):
        yield cur_date
        cur_date = cur_date + interval


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

    # Generate date list
    dates = [START_DATE + timedelta(days=x)
             for x in range(0, (END_DATE-START_DATE).days+1)]

    script_file = f"/mnt/batch/tasks/shared/{SCRIPT}"


    for cur_date in dates:
        #print(f"add tasks for {cur_date} ...")
        packs = list()

        # build source paths
        packs.append({
            'day': cur_date
        })

        # print(packs)

        try:

            # Add the tasks to the job.
            tasks = list()

            for pack in packs:
                tid = "task_{}".format(pack['day'].strftime("%Y%m%d"))

                commands = "/bin/bash -c \"python3 {} --vm-core {} --vm-ram {} --shuffle-partitions {} --storage '{}' --sas '{}' --container-in '{}' --container-out '{}'  --country '{}' --date '{}' \"".format(
                    script_file,
                    POOL_VM_CORE, POOL_VM_RAM, SHUFFLE_PARTITIONS,
                    STORAGE_ACCOUNT_NAME, SAS_TOKEN, INPUT_CONTAINER_NAME, OUTPUT_CONTAINER_NAME,
                    COUNTRY,
                    pack['day'].strftime("%Y-%m-%d"))

                tasks.append(
                    batch.models.TaskAddParameter(
                        id=tid,
                        command_line=commands,
                    )
                )

            batch_client.task.add_collection(JOB_ID, tasks)

            print("{} tasks submitted for {}".format(len(tasks), cur_date))

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
