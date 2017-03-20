import logging
import os
import sys
import json
import requests
from urllib.parse import urljoin
from time import sleep
import sesamclient

logger = None
overwrite_systems = False
overwrite_pipes = False
delete_pipes = True
update_interval = 1800


def assert_system(node, system_config):
    """
    Post the system config to the master node through the API
    :param master_node:
    :param system_config:
    :return:
    """

    system = node["api_connection"].get_system(system_config["_id"])
    if system is None:
        logger.info("Adding system '%s' to node %s" % (system_config["_id"], node["_id"]))
        master_node["api_connection"].add_systems([system_config])
    elif overwrite_systems:
        logger.info("Modifying existing system '%s' in the node %s" % (system_config["_id"], node["_id"]))
        system.modify(system_config)


def assert_slave_systems(master_node, slave_nodes):

    if not master_node["endpoint"].endswith("/"):
        master_node["endpoint"] += "/"

    logger.info("Master API endpoint is: %s" % master_node["endpoint"] + "api")

    master_node["api_connection"] = sesamclient.Connection(sesamapi_base_url=master_node["endpoint"] + "api",
                                                           jwt_auth_token=master_node["jwt_token"])



    for slave_node in slave_nodes:
        logger.info("Processing slave system '%s'.." % slave_node["_id"])

        for system in slave_node.get("managed_systems")

        system_config_master = {
                "_id": "slave-%s" % slave_node["_id"],
                "name": slave_node["_id"],
                "type": "system:url",
                "url_pattern": slave_node["endpoint"] + "api/datasets/%s/entities",
                "verify_ssl": True,
                "jwt_token": slave_node["jwt_token"],
                "authentication": "jwt",
                "connect_timeout": 60,
                "read_timeout": 7200
        }

        if not slave_node["endpoint"].endswith("/"):
            slave_node["endpoint"] += "/"

        if "api_connection" not in slave_node:
            logger.info("Slave '%s' API endpoint is: %s" % (slave_node["_id"], slave_node["endpoint"] + "api"))
            slave_node["api_connection"] = sesamclient.Connection(sesamapi_base_url=slave_node["endpoint"] + "api",
                                                                  jwt_auth_token=slave_node["jwt_token"])

        assert_system(master_node, system_config)

        # Remove any system not managed by the slave


def get_slave_node_datasets(slave_node):
    """
    Get the datasets we want to sync from the slave by reading its effective config
    :return:
    """

    old_datasets = slave_node.get("datasets", [])[:]

    all_source_datasets = []
    all_sink_datasets = []
    for pipe in slave_node["api_connection"].get_pipes():
        if pipe.id.startswith('system:'):
            continue

        source = pipe.config["effective"].get("source")
        sink = pipe.config["effective"].get("sink")

        sink_datasets = sink.get("datasets", sink.get("dataset"))
        if sink_datasets:
            if not isinstance(sink_datasets, list):
                all_sink_datasets.append(sink_datasets)
            else:
                all_sink_datasets.extend(sink_datasets)

        source_datasets = source.get("datasets", source.get("dataset"))
        if source_datasets:
            if not isinstance(source_datasets, list):
                source_datasets = [source_datasets]

            # Clean datasets, in case there are "merge" ones
            if source.get("type") == "merge":
                source_datasets = [ds.rpartition(" ")[0] for ds in source_datasets if ds]

            all_source_datasets.extend(source_datasets)

    all_sink_datasets = set([d for d in all_sink_datasets if not d.startswith('system:')])
    all_source_datasets = set([d for d in all_source_datasets if not d.startswith('system:')])

    # These datasets should exist as pipes in the master
    slave_node["datasets"] = all_sink_datasets.difference(all_source_datasets)

    # These datasets used to exist but don't anymore, we must remove the associated pipes from master
    slave_node["datasets_to_delete"] = set(old_datasets).difference(slave_node["datasets"])


def get_slave_datasets(slave_nodes):
    for slave_node in slave_nodes:
        get_slave_node_datasets(slave_node)


def assert_sync_pipes(master_node, slave_nodes):
    """
    Make sure all pipes that should exist does and all that refer to old datasets are deleted
    :param master_node:
    :param slave_nodes:
    :return:
    """

    # Get existing pipes for each slave and remove the ones that no longer exist in the slave and create/update the ones
    # that do

    for slave_node in slave_nodes:
        logger.info("Processing slave sync pipes for '%s'" % slave_node["_id"])

        # Delete pipes whose dataset used to exist in the slave but has been deleted since the last time we checked
        if delete_pipes:
            for dataset in slave_node.get("datasets_to_delete", []):
                # Check if it exists first, don't try to delete non-existing datasets
                pipe_id = "%s-from-slave-%s" % (dataset, slave_node["_id"])
                if master_node["api_connection"].get_pipe(pipe_id):
                    logger.info("Removing pipe '%s' from master because the dataset "
                                "in the slave has been removed" % pipe_id)
                    master_node["api_connection"].delete_pipe(pipe_id)

        # If the pipe doesn't exist, add it
        for dataset in slave_node.get("datasets", []):
            pipe_id = "%s-from-slave-%s" % (dataset, slave_node["_id"])

            pipe_config = {
                "_id": pipe_id,
                "type": "pipe",
                "add_namespaces": False,
                "source": {
                    "type": "json",
                    "system": "slave-%s" % slave_node["_id"],
                    "url": dataset,
                    "supports_since": True,
                    "is_chronological": True
                },
                "sink": {
                    "type": "dataset",
                    "dataset": dataset
                },
                "pump": {
                    "schedule_interval": slave_node.get("sync_interval", 300)
                }
            }

            pipe = master_node["api_connection"].get_pipe(pipe_id)
            if pipe is None:
                # New pipe - post it
                logger.info("Adding new sync pipe '%s' to the master" % pipe_id)
                master_node["api_connection"].add_pipes([pipe_config])
            elif overwrite_pipes:
                # The pipe exists, so update it (in case someone has modified it)
                logger.info("Modifying existing pipe '%s' in the master" % pipe_id)
                pipe.modify(pipe_config)


if __name__ == '__main__':
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger('data-sync-agent-service')

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    logger.setLevel(logging.DEBUG)

    # Read config from env vars

    if "MASTER_NODE" not in os.environ:
        logger.error("MASTER_NODE configuration missing!")
        sys.exit(1)

    if "UPDATE_INTERVAL" in os.environ:
        try:
            update_interval = int(os.environ.get("UPDATE_INTERVAL"))
            logger.info("Setting update interval to %s" % update_interval)
        except:
            logger.warning("Update interval is not an integer! Falling back to default")

    master_node = json.loads(os.environ["MASTER_NODE"])

    if not "SLAVE_NODES" in os.environ:
        logger.error("SLAVE_NODES configuration missing!")
        sys.exit(1)

    slave_nodes = json.loads(os.environ["SLAVE_NODES"])

    while True:
        assert_slave_systems(master_node, slave_nodes)

        orchestrate_pipes(master_node, slave_nodes)

        # Sleep for a while then go again
        logger.info("Master updated to sync from slaves, sleeping for %s seconds..." % update_interval)

        sleep(update_interval)
