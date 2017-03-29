=============================
Orchestrator 1.0 microservice
=============================

A microservice that consumes config from a master node and orchestrates its system and pipes to one or more
slave nodes. The slave nodes then sync their data back to the master node.

*NOTE: this service assumes all systems and pipes on the slave nodes are managed by the service - do not create or edit
pipes or systems manually in the slave nodes!*

System configuration in Sesam:

::

    {
        "_id": "orchestrator-microservice",
        "name": "Name of data sync microservice",
        "type": "system:microservice",
        "docker": {
            "image": "sesam/orchestrator-1.0:latest",
            "port": 5000,
            "memory": 128,
            "environment": {
                "UPDATE_INTERVAL": "1800",
                "MASTER_NODE": {
                    "_id" : "m1",
                    "endpoint" : "https://m1.sesam.cloud",
                    "jwt_token" : "fklrl464nimsnfskfklrl464nimskfklrl464nimsnfskfkfklrl464nimsnfskf4nimsnfskfklrl464n",
                    "jwt_secret_key": "foo"
                },
                "SLAVE_NODES": [
                    {
                        "_id" : "s1",
                        "endpoint" : "https://s1.sesam.cloud",
                        "managed_systems" : ["my-oracle-system"],
                        "jwt_token" : "msnfskfklrl464nimsnfskfklrl464nimsnfskfklrl464nimsnfskfklrl44nimsnfskfklrl464ni",
                        "jwt_secret_key": "bar",
                        "sync_interval": 300,
                        "sync_interval_stagger_range": 300
                    },
                    {
                        "_id" : "s2",
                        "endpoint" : "https://s2.sesam.cloud",
                        "managed_systems" : ["my-url-system"],
                        "jwt_secret_key": "baz",
                        "jwt_token" : "msnfskfklrl464nimsnfskfklrl464nimsnfskfklrl464nimsnfskfklrl464nimsnfskfklrl464n"
                    }
                ]
            }
        }
    }

The microservice operates under the following assumptions/restrictions:

* The master and slave(s) must share the same secrets (if any) - the system config is shared between the nodes
* The environment variables will be synced from master to slave (it will overwrite any in the slaves with the same name).
  Note that to be able to syncronise environment variables, the JWT tokens must be issued to the "group:Admin" group as it
  needs access to APIs restricted to this group
* Make sure no slaves state the same managed system id - the service will refuse to run in this scenario
* If you change the config and move a system managed by a slave node to another slave, the system and pipe(s)
  in the original slave will be copied to the new slave and the originals deleted. The synchronising pipe in the master
  will also be overwritten and reset. Additionally, the dataset(s) in the original slave node will be deleted.
* Any master pipe with a system matching any slave node's ``managed_system`` property will be moved to the corresponding
  slave at any time after it is saved or uploaded to the master. It will then be replaced by a synchronising pipe in the master
  using the same ``_id`` - however, the original configuration is kept in the "metadata" property of the synchronising pipe.
* Do not hand edit any pipes created by the orchestrator as they can and will be overwritten by the orchestrator service at any time
* The pipes that pull data from the slaves to the node will run every ``sync_interval`` seconds (default 300), staggered by
  a random amount controlled by ``sync_interval_stagger_range`` (default the same as ``sync_interval``). The staggering
  is to avoid a "stampeding herd" of simultaneously running threads in the master reading data from a slave node.
