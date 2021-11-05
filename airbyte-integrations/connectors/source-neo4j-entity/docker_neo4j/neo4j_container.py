#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import os
import sys
import time

from neo4j import GraphDatabase
from testcontainers.compose import DockerCompose


class Neo4jTestInstance(object):

    def __init__(self, docker_compose_filepath, docker_compose_filename = "docker-compose.yml") -> None:
        self.compose = DockerCompose(
            filepath=docker_compose_filepath,
            compose_file_name=[docker_compose_filename],
            pull=True
        )
        self._config = {}
    

    @property
    def config(self):
        """
        Neo4j's driver configuration
        """
        return self._config


    def start(self):
        """
        Start neo4j container and wait until neo4j is ready to handle requests
        """
        self.compose.start()
        
        self._config = {
            "scheme": "bolt",
            "port": 7687,
            "host": self.compose.get_service_host("neo4j", 7687),
            "username": "neo4j",
            "password": "test"
        }

        self.driver = GraphDatabase.driver(
            "{}://{}:{}".format(self._config["scheme"], self._config["host"], self._config["port"]),
            auth=(self._config["username"], self._config["password"])
        )

        self._wait_database_ready(max_time = 15)
        print("Neo4j container is ready to be used")


    def stop(self):
        """
        Stop neo4j container
        """
        self.compose.stop()


    def _wait_database_ready(self, max_time = 15):
        """
        Wait until the database is ready to handle requests
        It checks connectivity every second until max time
        """
        try:
            self.driver.verify_connectivity()
        except Exception as e:
            if max_time == 0:
                raise e
            
            max_time = max_time - 1
            time.sleep(1)
            self._wait_database_ready(max_time=max_time)


    @staticmethod
    def neo4j_create_nodes_tx(tx, labels, properties):
        """
        Create nodes
        """
        return tx.run("CALL apoc.create.nodes($labels, $properties)", labels=labels, properties=properties)


    @staticmethod
    def neo4j_create_relationship_tx(tx, from_label, from_primary_key, to_label, to_primary_key, type, properties):
        """
        Create a relationship between two nodes
        """
        query = """
        MATCH (n:{} {{primary_key: {}}})
        MATCH (m:{} {{primary_key: {}}})
        CALL apoc.create.relationship(n, $type, $properties, m)
        YIELD rel
        RETURN rel;
        """.format(
            from_label, from_primary_key,
            to_label, to_primary_key
        )
        return tx.run(query, type=type, properties=properties)


    def populate_test_data(self):
        """
        Populate neo4j with test data
        """
        # creates nodes node_1 (multi labels)
        node_properties = []
        node_labels = ["node_1", "node_3"]
        ids = range(1, 101, 1)
        timestamps = range(1633103553000, 1633107843000, 43000) # generates 100 values

        for id in ids:
            node_properties.append({"primary_key": id, "prop_1": "value {}".format(id), "updated_at": timestamps[id - 1]})

        with self.driver.session() as session:
            session.write_transaction(self.neo4j_create_nodes_tx, node_labels, node_properties)

        # creates nodes node_2 (single label)
        node_properties = []
        node_labels = ["node_2"]
        ids = range(1, 51, 1)
        timestamps = range(1633103553000, 1635447843000, 86400) # generates 50 values

        for id in ids:
            node_properties.append({"primary_key": id, "prop_2": "value {}".format(id), "prop_3": [1, 3, 5, 6], "updated_at": timestamps[id - 1]})

        with self.driver.session() as session:
            session.write_transaction(self.neo4j_create_nodes_tx, node_labels, node_properties)

        # create relationships
        rel_properties = {}
        rel_type = "rel_1"

        with self.driver.session() as session:
            for id in ids:
                session.write_transaction(
                    self.neo4j_create_relationship_tx,
                    from_label="node_2",
                    from_primary_key=id,
                    to_label="node_1",
                    to_primary_key=id,
                    type=rel_type,
                    properties=rel_properties)



if __name__ == "__main__":
    if sys.argv[1] not in ["start", "stop"]:
        print("Command must be 'start' or 'stop'. Unknow command '{}'.".format(sys.argv[1]))

    neo4j = Neo4jTestInstance(docker_compose_filepath=os.path.dirname(__file__))

    if sys.argv[1] == "start":
        neo4j.start()
        neo4j.populate_test_data()

    elif sys.argv[1] == "stop":
        neo4j.stop()
