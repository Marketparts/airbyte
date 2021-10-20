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

from pytest import fixture

from neo4j import BoltDriver, Neo4jDriver

from source_neo4j_entity.neo4j import Neo4jClient

@fixture
def mock_neo4j_client_fetch_results(mocker):
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter(['alpha1', "alpha2", "alpha3"]))


def test_uri():
    config = {"scheme": "myscheme", "host": "myhostname", "port": "myport"}
    client = Neo4jClient(config=config)
    
    expected_result = "myscheme://myhostname:myport"
    assert client.uri == expected_result


def test_driver():
    config = {"scheme": "bolt", "host": "myhostname", "port": 7687, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)
    
    assert isinstance(client.driver, BoltDriver)

    config = {"scheme": "neo4j", "host": "myhostname", "port": 7687, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)
    
    assert isinstance(client.driver, Neo4jDriver)


def test_node_labels(mock_neo4j_client_fetch_results):
    config = {"scheme": "bolt", "host": "myhostname", "port": 7687, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)

    assert len(client.node_labels) == 3
    assert all([a == b for a, b in zip(client.node_labels, ['alpha1', "alpha2", "alpha3"])])


def test_relationship_types(mock_neo4j_client_fetch_results):
    config = {"scheme": "bolt", "host": "myhostname", "port": 7687, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)

    assert len(client.relationship_types) == 3
    assert all([a == b for a, b in zip(client.relationship_types, ['alpha1', "alpha2", "alpha3"])])


def test_property_keys(mock_neo4j_client_fetch_results):
    config = {"scheme": "bolt", "host": "myhostname", "port": 7687, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)

    assert len(client.property_keys) == 3
    assert all([a == b for a, b in zip(client.property_keys, ['alpha1', "alpha2", "alpha3"])])
