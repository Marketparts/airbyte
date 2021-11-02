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
import time
import random

import pytest

from testcontainers.compose import DockerCompose
from neo4j import GraphDatabase


def neo4j_create_nodes_tx(tx, labels, properties):
    """
    Create nodes
    """
    return tx.run("CALL apoc.create.nodes($labels, $properties)", labels=labels, properties=properties)

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


@pytest.fixture(scope="session", autouse=True)
def neo4j_container():
    """ This fixture is a placeholder for external resources that acceptance test might require."""

    # lauch docker compose to get a neo4j container
    try:
        compose = DockerCompose(
            filepath=os.path.dirname(__file__),
            compose_file_name=["docker-compose.yml"],
            pull=True
        )
        compose.start()
        
        port = 7687
        host = compose.get_service_host("neo4j-airbyte-test", port)
        username = "neo4j"
        password = "test"
        
        pytest.neo4j_client_config = {"scheme": "bolt", "host": host, "port": port, "username": username, "password": password}

        # wait 10s to let the neo4j start
        time.sleep(15)
        
        driver = GraphDatabase.driver(
            "bolt://{}:{}".format(host, port),
            auth=(username, password)
        )
        driver.verify_connectivity()


        node_properties = []
        node_labels = ["node_1", "node_3"]
        ids = range(0, 100, 1)
        timestamps = range(1633103553000, 1633107843000, 43000) # generates 100 values

        for id in ids:
            node_properties.append({"primary_key": id, "prop_1": "value {}".format(id), "updated_at": timestamps[id]})

        with driver.session() as session:
            session.write_transaction(neo4j_create_nodes_tx, node_labels, node_properties)

        node_properties = []
        node_labels = ["node_2"]
        ids = range(200, 250, 1)
        timestamps = range(1633103553000, 1635447843000, 86400) # generates 50 values

        for id in ids:
            node_properties.append({"primary_key": id, "prop_2": "value {}".format(id), "prop_3": [1, 3, 5, 6], "updated_at": timestamps[id]})

        with driver.session() as session:
            session.write_transaction(neo4j_create_nodes_tx, node_labels, node_properties)



        rel_properties = {}
        rel_type = "rel_1"
        with driver.session() as session:
            for id in ids:
                session.write_transaction(
                    neo4j_create_relationship_tx,
                    from_label="node_2",
                    from_primary_key=id,
                    to_label="node_1",
                    to_primary_key=id - 200,
                    type=rel_type,
                    properties=rel_properties)


        
    except Exception as e:
        # stop the container before raising the exception
        compose.stop()

        # wait 10s to let neo4j stop
        time.sleep(10)

        raise e

    yield
    compose.stop()

    
