import logging
import os
import sys
import json
import requests
from urllib.parse import urljoin
from time import sleep
import sesamclient
from copy import copy
from random import randint
from pprint import pformat

logger = None
overwrite_systems = False
overwrite_pipes = False
delete_pipes = True
update_interval = 1800


def assert_system(node, system_config):
    """
    Post the system config to the node through the API
    :param node:
    :param system_config:
    :return:
    """

    system = node["api_connection"].get_system(system_config["_id"])
    if system is None:
        logger.info("Adding system '%s' to node %s" % (system_config["_id"], node["_id"]))
        node["api_connection"].add_systems([system_config])
    else:
        logger.info("Modifying existing system '%s' in the node %s" % (system_config["_id"], node["_id"]))
        system.modify(system_config)


def assert_pipe(node, pipe_config):
    """
    Post the pipe config to the node through the API
    :param node:
    :param pipe_config:
    :return:
    """

    pipe = node["api_connection"].get_pipe(pipe_config["_id"])
    if pipe is None:
        logger.info("Adding new pipe '%s' to node %s" % (pipe_config["_id"], node["_id"]))
        node["api_connection"].add_pipes([pipe_config])
    else:
        logger.info("Modifying existing pipe '%s' in the node %s" % (pipe_config["_id"], node["_id"]))
        pipe.modify(pipe_config)


def get_node_pipes_and_systems(node):
    systems = {}

    for system in node["api_connection"].get_systems():
        if system.id.startswith("system:"):
            continue

        if system.id not in systems:
            systems[system.id] = []

    for pipe in node["api_connection"].get_pipes():
        if pipe.id.startswith("system:"):
            continue

        effective_config = pipe.config.get("effective")
        if effective_config:
            source = effective_config.get("source")
            sink = effective_config.get("sink")

            if source:
                system_id = source["system"]
                if system_id.startswith("system:"):
                    continue

                if system_id not in systems:
                    systems[system_id] = []

                if pipe not in systems[system_id]:
                    systems[system_id].append(pipe)

            if sink:
                system_id = sink["system"]
                if system_id.startswith("system:"):
                    continue

                if system_id not in systems:
                    systems[system_id] = []

                if pipe not in systems[system_id]:
                    systems[system_id].append(pipe)

    return systems


def delete_node_system(node, system_id, pipes):
    # Delete pipes and datasets associated with system and then the system itself
    logger.info("Deleting system '%s' and all its associated pipes and "
                "datasets from node '%s'" % (system_id, node["_id"]))
    for pipe in pipes:
        effective_config = pipe.config.get("effective")
        if effective_config:
            sink = effective_config.get("sink")
            if sink:
                sink_datasets = sink.get("datasets", sink.get("dataset"))
                if sink_datasets and not isinstance(sink_datasets, list):
                    sink_datasets = [sink_datasets]

                logger.info("Deleting datasets: %s" % sink_datasets)
                for dataset_id in sink_datasets:
                    dataset = node["api_connection"].get_dataset(dataset_id)
                    if dataset:
                        logger.info("Deleting dataset '%s' in in node '%s'.." % (dataset_id, node["_id"]))
                        dataset.delete()
                    else:
                        logger.warning("Failed to delete dataset '%s' in in node '%s' "
                                       "- could not find dataset" % (dataset_id, node["_id"]))

        logger.info("Deleting pipe '%s' in in node '%s'.." % (pipe.id, node["_id"]))
        pipe.delete()

    system = node["api_connection"].get_system(system_id)
    if system:
        logger.info("Deleting system '%s' in in node '%s'.." % (system.id, node["_id"]))
        system.delete()
    else:
        logger.warning("Failed to delete system '%s' in in node '%s' "
                       "- could not find system" % (system.id, node["_id"]))


def delete_node_pipe(node, pipe_id):
    # Delete pipes and all associated datasets from the node
    logger.info("Deleting pipe '%s' and all its associated "
                "datasets from node '%s'" % (pipe_id, node["_id"]))

    pipe = node["api_connection"].get_pipe(pipe_id)
    if pipe is not None:
        effective_config = pipe.config.get("effective")
        if effective_config:
            sink = effective_config.get("sink")
            if sink:
                sink_datasets = sink.get("datasets", sink.get("dataset"))
                if sink_datasets and not isinstance(sink_datasets, list):
                    sink_datasets = [sink_datasets]

                logger.info("Deleting datasets: %s" % sink_datasets)
                for dataset_id in sink_datasets:
                    dataset = node["api_connection"].get_dataset(dataset_id)
                    if dataset:
                        logger.info("Deleting dataset '%s' in in node '%s'.." % (dataset_id, node["_id"]))
                        dataset.delete()
                    else:
                        logger.warning("Failed to delete dataset '%s' in in node '%s' "
                                       "- could not find dataset" % (dataset_id, node["_id"]))

        logger.info("Deleting pipe '%s' in in node '%s'.." % (pipe.id, node["_id"]))
        pipe.delete()
    else:
        logger.warning("Failed to delete pipe '%s' in in node '%s' "
                       "- could not find pipe" % (pipe_id, node["_id"]))


def stagger_time(scheduled_time, stagger_range=None):
    if stagger_range is None:
        stagger_range = scheduled_time

    return scheduled_time + randint(0, stagger_range - 1)


def move_pipes(target_node, pipes, pipe_type="json"):
    # Move any non-orchestrated pipes in list to the slave
    # Stop master pipes before we do anything

    stop_and_disable_pipes(pipes)

    for pipe in pipes:
        original_pipe_config = pipe.config.get("original")
        pipe_metadata = original_pipe_config.get("metadata", {})

        if "orchestrator" not in pipe_metadata:
            # We have not seen this one before, or it has been changed

            logger.info("Moving pipe '%s' from master to slave '%s'" % (pipe.id, target_node["_id"]))
            assert_pipe(target_node, original_pipe_config)

            # Rewrite pipe config
            sync_pipe_config = {
                "_id": pipe.id,
                "type": "pipe",
                "add_namespaces": False,
                "source": {
                    "type": pipe_type,
                    "system": pipe.config["effective"]["source"]["system"],
                    "url":  pipe.id,
                    "supports_since": True,
                    "is_chronological": True,
                    "page_size": 50000
                },
                "sink": {
                    "type": "dataset",
                    "dataset":  pipe.id
                },
                "pump": {
                    "schedule_interval": stagger_time(target_node.get("sync_interval", 300),
                                                      target_node.get("sync_interval_stagger_range"))
                },
                "metadata": {
                    "description": "This pipe was generated by the orchestrator service, do not edit it",
                    "orchestrator": {
                        "slave": target_node["_id"],
                        "url": target_node["endpoint"] + "api/pipes/" + pipe.id,
                        "original_configuration": original_pipe_config
                    }
                }
            }

            logger.info("Rewriting pipe '%s' in master.." % pipe.id)
            pipe.modify(sync_pipe_config)

            logger.info("Enabling and resetting pipe '%s' in master.." % pipe.id)
            pump = pipe.get_pump()
            if "enable" in pump.supported_operations:
                pump.enable()

            #if "update-last-seen" in pump.supported_operations:
            #    pump.unset_last_seen()


def stop_and_disable_pipes(pipes):
    for pipe in pipes:
        pump = pipe.get_pump()
        # Stop the pipe
        if "stop" in pump.supported_operations:
            pump.stop()

        if "disable" in pump.supported_operations:
            pump.disable()


def get_orchestrator_metadata(component):
    return component.config.get("original", {}).get("metadata", {}).get("orchestrator")


def is_orchestrated(component):
    orchestrator = get_orchestrator_metadata(component)
    return orchestrator is not None


def orchestrate_pipes(master_node, slave_nodes):
    if master_node.get("use_binary_source", False):
        pipe_type = "binary"
    else:
        pipe_type = "json"

    master_systems = get_node_pipes_and_systems(master_node)
    master_pipes = {}
    for system_id in master_systems:
        for pipe in master_systems[system_id]:
            if pipe.id not in master_pipes:
                master_pipes[pipe.id] = system_id

    for slave_node in slave_nodes:
        logger.info("Processing slave '%s'.." % slave_node["_id"])

        # All existing systems and pipes in the slave
        slave_systems = get_node_pipes_and_systems(slave_node)

        # Delete any system and related pipes/datasets that are not supposed to be manged by this slave
        slave_managed_systems = slave_node.get("managed_systems", [])

        # Delete any systems not managed by the slave, or which doesn't exist in the master
        for slave_system_id in slave_systems:
            if slave_system_id not in slave_managed_systems or slave_system_id not in master_systems and \
                    not slave_system_id.startswith("sesam-dev-test"):
                delete_node_system(slave_node, slave_system_id, slave_systems[slave_system_id])

            # Check if the slave pipes exists in master
            for pipe in slave_systems[slave_system_id]:
                if pipe.id not in master_pipes and not pipe.id.startswith('sesam-dev-test'):
                    # Someone deleted the pipe in master, so remove it (and any dataset) from the slave
                    delete_node_pipe(slave_node, pipe.id)

        for managed_system_id in slave_managed_systems:
            # Check if the managed system actually exists in master
            master_sys = master_node["api_connection"].get_system(managed_system_id)

            # Check if it is something we've written already, or if its something new
            if master_sys is not None:
                pipes_to_move = []
                for pipe in master_systems[managed_system_id]:
                    if is_orchestrated(pipe):
                        pipe_metadata = get_orchestrator_metadata(pipe)
                        if pipe_metadata["slave"] != slave_node["_id"]:
                            # This pipe is orchestrated but previously managed by another slave - restore the
                            # original config and move it to the new slave
                            logger.warning("Moving previously orchestrated pipe '%s' "
                                           "to new slave '%s'" % (pipe.id, slave_node["_id"]))
                            stop_and_disable_pipes([pipe])
                            pipe.modify(pipe_metadata["original_configuration"])
                            pipes_to_move.append(pipe)
                    else:
                        pipes_to_move.append(pipe)

                update_master_system = False
                if is_orchestrated(master_sys):
                    system_metadata = get_orchestrator_metadata(master_sys)
                    if system_metadata["slave"] != slave_node["_id"]:
                        # This system is orchestrated but was previously managed by another slave - restore the
                        # original config so we can move it to the new slave
                        logger.warning("Moving previously orchestrated system '%s' "
                                       "to new slave '%s'" % (managed_system_id, slave_node["_id"]))
                        master_sys.modify(system_metadata["original_configuration"])
                        update_master_system = True
                else:
                    update_master_system = True

                if update_master_system:
                    logger.info("Found new or updated system to manage: '%s'" % managed_system_id)

                    # New or updated system
                    slave_config = copy(master_sys.config["original"])

                    if slave_node.get("jwt_secret_key"):
                        jwt_token = "$SECRET(%s)" % slave_node["jwt_secret_key"]
                    else:
                        jwt_token = slave_node["jwt_token"]

                    system_config_master = {
                        "_id": managed_system_id,
                        "name": master_sys.config.get("name", managed_system_id),
                        "type": "system:url",
                        "url_pattern": slave_node["endpoint"] + "api/datasets/%s/entities?history=false",
                        "verify_ssl": True,
                        "jwt_token": jwt_token,
                        "authentication": "jwt",
                        "connect_timeout": 60,
                        "read_timeout": 7200,
                        "metadata": {
                            "description": "This system was generated by the orchestrator service, do not edit it",
                            "orchestrator": {
                                "slave": slave_node["_id"],
                                "url": slave_node["endpoint"] + "api/systems/" + managed_system_id,
                                "original_configuration": slave_config
                            }
                        }
                    }

                    # Add or update system in slave
                    logger.info("Moving system '%s' to slave node '%s'" % (managed_system_id, slave_node["_id"]))
                    assert_system(slave_node, slave_config)

                    if pipes_to_move:
                        logger.info("Stopping and disabling candidate pipes for systemn '%s'" % managed_system_id)
                        stop_and_disable_pipes(pipes_to_move)

                    # Update system in master
                    logger.info("Rewriting system '%s' in master..." % managed_system_id)
                    assert_system(master_node, system_config_master)

                if pipes_to_move:
                    logger.info("Moving %d pipes from master to slave '%s'" % (len(pipes_to_move), slave_node["_id"]))
                    move_pipes(slave_node, pipes_to_move, pipe_type=pipe_type)
            else:
                logger.warning("Slave '%s' asked to manage non-existant "
                               "system '%s'" % (slave_node["_id"], managed_system_id))


def assert_non_overlapping_managed_systems(slave_nodes):
    managed_systems = []

    for slave_node in slave_nodes:
        slave_managed_systems = list(set(slave_node.get("managed_systems", [])))

        for slave_system_id in slave_managed_systems:
            if slave_system_id in managed_systems:
                logger.error("Slaves cannot manage the same systems! Orchestrator cannot continue.")
                sys.exit(1)


def copy_environment_variables(master_node, slave_nodes):
    try:
        env_vars = master_node["api_connection"].get_env_vars()

        if env_vars:
            for slave_node in slave_nodes:
                slave_env_vars = slave_node["api_connection"].get_env_vars()
                if slave_env_vars != env_vars:
                    logger.debug("Master env vars:\n%s" % pformat(env_vars))
                    logger.debug("Slave env vars:\n%s" % pformat(slave_env_vars))
                    logger.info("Master and slave env vars are different - copying env vars from master "
                                "to slave node %s" % slave_node["_id"])
                    slave_node["api_connection"].put_env_vars(env_vars)
    except BaseException as e:
        logger.exception("Copying env vars from master to slave node failed. Make sure the JWT tokens used "
                         "are issued to 'group:Admin'!")


def assert_same_secret_keys(master_node, slave_nodes):

    master_secret_keys = set(master_node["api_connection"].get_secrets())
    logger.debug("master secrets: %s" % master_secret_keys)

    can_run = True
    for slave_node in slave_nodes:
        slave_secret_keys = set(slave_node["api_connection"].get_secrets())
        logger.debug("Slave '%s' secrets: %s" % (slave_node["_id"], slave_secret_keys))
        if not master_secret_keys.issubset(slave_secret_keys):
            logger.error("Slave node '%s' is missing secret keys from master!" % slave_node["_id"])
            can_run = False

    if not can_run:
        logger.warning("Master and slave secrets mismatch!")
        #logger.error("Master and slave secrets mismatch, exiting")
        #sys.exit(1)


if __name__ == '__main__':
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger('orchestrator-service')

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

    if "SLAVE_NODES" not in os.environ:
        logger.error("SLAVE_NODES configuration missing!")
        sys.exit(1)

    slave_nodes = json.loads(os.environ["SLAVE_NODES"])

    if not master_node["endpoint"].endswith("/"):
        master_node["endpoint"] += "/"

    logger.info("Master API endpoint is: %s" % master_node["endpoint"] + "api")

    master_node["api_connection"] = sesamclient.Connection(sesamapi_base_url=master_node["endpoint"] + "api",
                                                           jwt_auth_token=master_node["jwt_token"],
                                                           timeout=60*10)

    for slave_node in slave_nodes:
        if not slave_node["endpoint"].endswith("/"):
            slave_node["endpoint"] += "/"

        if "api_connection" not in slave_node:
            logger.info("Slave '%s' API endpoint is: %s" % (slave_node["_id"], slave_node["endpoint"] + "api"))
            slave_node["api_connection"] = sesamclient.Connection(sesamapi_base_url=slave_node["endpoint"] + "api",
                                                                  jwt_auth_token=slave_node["jwt_token"],
                                                                  timeout=60*10)
    assert_non_overlapping_managed_systems(slave_nodes)

    while True:
        try:
            logger.info("Syncing master and slaves...")

            try:
                logger.info("Making sure the node is up...")
                systems = master_node["api_connection"] .get_systems()
                pipes = master_node["api_connection"] .get_pipes()
            except BaseException as e:
                raise AssertionError("Wasn't able to talk to the master node. Aborting this run.")

            if len(systems) == 0:
                raise AssertionError("Master returned 0 systems - surely this is an error?")

            if len(pipes) == 0:
                raise AssertionError("Master returned 0 pipes - surely this is an error?")

            assert_same_secret_keys(master_node, slave_nodes)

            copy_environment_variables(master_node, slave_nodes)

            orchestrate_pipes(master_node, slave_nodes)

            # Sleep for a while then go again
            logger.info("Master updated to sync from slaves, sleeping for %s seconds..." % update_interval)
        except BaseException as e:
            logger.exception("Error while orchestrating!")

        sleep(update_interval)
