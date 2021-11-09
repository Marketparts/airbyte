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

import pytest
from unittest.mock import PropertyMock

from neo4j import BoltDriver

from source_neo4j_entity.neo4j import Neo4jClient

@pytest.fixture
def neo4j_client_config():
    # use a real hostname as neo4j driver will try to resolve DNS hostname 
    return {"scheme": "bolt", "host": "google.com", "port": 80, "username": "myusername", "password": "mypassword"}


def test_init(neo4j_client_config):
    client = Neo4jClient(config=neo4j_client_config)
    
    assert client._config == neo4j_client_config


def test_uri(neo4j_client_config):
    client = Neo4jClient(config=neo4j_client_config)
    
    expected_result = "{}://{}:{}".format(neo4j_client_config["scheme"], neo4j_client_config["host"], neo4j_client_config["port"])
    assert client.uri == expected_result


def test_driver(neo4j_client_config):
    client = Neo4jClient(config=neo4j_client_config)
    
    assert isinstance(client.driver, BoltDriver)


def test_node_labels(mocker, neo4j_client_config):
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter(['alpha1', "alpha2", "alpha3"]))
    client = Neo4jClient(config=neo4j_client_config)

    assert len(client.node_labels) == 3
    assert all([a == b for a, b in zip(client.node_labels, ['alpha1', "alpha2", "alpha3"])])


def test_relationship_types(mocker, neo4j_client_config):
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter(['alpha1', "alpha2", "alpha3"]))
    client = Neo4jClient(config=neo4j_client_config)

    assert len(client.relationship_types) == 3
    assert all([a == b for a, b in zip(client.relationship_types, ['alpha1', "alpha2", "alpha3"])])


def test_property_keys(mocker, neo4j_client_config):
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter(['alpha1', "alpha2", "alpha3"]))
    client = Neo4jClient(config=neo4j_client_config)

    assert len(client.property_keys) == 3
    assert all([a == b for a, b in zip(client.property_keys, ['alpha1', "alpha2", "alpha3"])])

def test_verify_connectivity(mocker, neo4j_client_config):
    mocker.patch.object(Neo4jClient, "verify_connectivity", return_value="myvalue")
    client = Neo4jClient(config=neo4j_client_config)
    
    assert client.verify_connectivity() == "myvalue"

def test_map_types_from_neo4j_to_json_schema_success(neo4j_client_config):
    client = Neo4jClient(config=neo4j_client_config)

    input_types = "String"
    expected_types = ["string"]
    assert client._map_types_from_neo4j_to_json_schema(input_types) == expected_types


    input_types = list(client.MAP_NEO4J_TYPES_TO_JSON_SCHEMA_TYPES.keys())
    expected_types = list(client.MAP_NEO4J_TYPES_TO_JSON_SCHEMA_TYPES.values())
    assert client._map_types_from_neo4j_to_json_schema(input_types) == expected_types

def test_map_types_from_neo4j_to_json_schema_failure(neo4j_client_config):
    client = Neo4jClient(config=neo4j_client_config)

    input_types = ["String", "UnknwonType"]
    with pytest.raises(ValueError) as e:
        client._map_types_from_neo4j_to_json_schema(input_types)

def test_get_node_json_schemas_v2(mocker, neo4j_client_config):
    mocker.patch.object(Neo4jClient, "node_labels", new_callable=PropertyMock, return_value =["node_1", "node_2", "node_3", "node_4"])

    fetch_results_returned = [
        {"propertyName": "alpha", "nodeLabels": ["node_1", "node_3"], "propertyTypes": ["String", "Double"], "mandatory": False},
        {"propertyName": "beta", "nodeLabels": ["node_1"], "propertyTypes": ["String"], "mandatory": True},
        {"propertyName": "gamma", "nodeLabels": ["node_2"], "propertyTypes": ["StringArray"], "mandatory": True}
        ]
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter(fetch_results_returned))

    expected_results = {
        "node_1": {
            "alpha" : {"type": ["string", "number"], "required": False},
            "beta" : {"type": ["string"], "required": True},
        },
        "node_2": {
            "gamma" : {"type": ["array"], "required": True}
        },
        "node_3": {
            "alpha" : {"type": ["string", "number"], "required": False}
        },
        "node_4": {}
    }

    client = Neo4jClient(config=neo4j_client_config)
    assert client._get_node_json_schemas_v2() == expected_results


def test_get_relationship_json_schemas_v2(mocker, neo4j_client_config):
    mocker.patch.object(Neo4jClient, "relationship_types", new_callable=PropertyMock, return_value =["rel_1", "rel_2", "rel_3"])

    fetch_results_returned = [
        {"propertyName": "alpha", "relType": ":`rel_1`", "propertyTypes": ["String", "Double"], "mandatory": False},
        {"propertyName": "beta", "relType": ":`rel_1`", "propertyTypes": ["String"], "mandatory": True},
        {"propertyName": "gamma", "relType": ":`rel_2`", "propertyTypes": ["StringArray"], "mandatory": True}
        ]
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter(fetch_results_returned))

    expected_results = {
        "rel_1": {
            "alpha" : {"type": ["string", "number"], "required": False},
            "beta" : {"type": ["string"], "required": True},
        },
        "rel_2": {
            "gamma" : {"type": ["array"], "required": True}
        },
        "rel_3": {}
    }

    client = Neo4jClient(config=neo4j_client_config)
    assert client._get_relationship_json_schemas_v2() == expected_results