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

from neo4j import BoltDriver

from source_neo4j.neo4j import Neo4jClient


def test_init(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)
    
    assert client._config == pytest.neo4j_client_config


def test_uri(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)
    
    expected_result = "{}://{}:{}".format(pytest.neo4j_client_config["scheme"], pytest.neo4j_client_config["host"], pytest.neo4j_client_config["port"])
    assert client.uri == expected_result


def test_driver(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)
    
    assert isinstance(client.driver, BoltDriver)


def test_node_labels(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    assert len(client.node_labels) == 4
    assert all([a == b for a, b in zip(client.node_labels, ['node_1', "node_2", "node_3", "node_4"])])


def test_relationship_types(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    assert len(client.relationship_types) == 2
    assert all([a == b for a, b in zip(client.relationship_types, ['rel_1', 'rel_2'])])


def test_property_keys(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    assert len(client.property_keys) == 8
    assert all([a == b for a, b in zip(client.property_keys, ['primary_key', "prop_1", "prop_2", "prop_3", "prop_4", "prop_5", "prop_6", "updated_at"])])

def test_verify_connectivity(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)
    try:
        client.verify_connectivity()
    except Exception as e:
        pytest.fail("Connectivity error {}".format(str(e)))


def test_get_node_json_schemas_v2(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    expected_results = {
        "node_1": {
            "primary_key" : {"type": ["number"], "required": True},
            "prop_1" : {"type": ["string"], "required": True},
            "updated_at" : {"type": ["number"], "required": True},
        },
        "node_2": {
            "primary_key" : {"type": ["number"], "required": True},
            "prop_2" : {"type": ["string"], "required": True},
            "prop_3" : {"type": ["array"], "required": True},
            "updated_at" : {"type": ["number"], "required": True},
        },
        "node_3": {
            "primary_key" : {"type": ["number"], "required": True},
            "prop_1" : {"type": ["string"], "required": True},
            "updated_at" : {"type": ["number"], "required": True},
        },
        "node_4": {
            "primary_key" : {"type": ["number"], "required": True},
            "prop_4" : {"type": ["string"], "required": True},
            "prop_5" : {"type": ["array"], "required": True},
            "updated_at" : {"type": ["number"], "required": True},
        }
    }

    assert client._get_node_json_schemas_v2() == expected_results


def test_get_relationship_json_schemas_v2(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    expected_results = {
        "rel_1": {},
        "rel_2": {
            "prop_6": {"type": ["number"], "required": True}
        }
    }

    assert client._get_relationship_json_schemas_v2() == expected_results