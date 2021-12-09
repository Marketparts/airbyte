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

from source_neo4j.source import SourceNeo4j
from unittest.mock import MagicMock, PropertyMock

from source_neo4j.neo4j import Neo4jClient
from source_neo4j.streams import IncrementalNeo4jStream, NodeStream, RelationshipStream


@pytest.fixture
def config_mock():
    # use a real hostname as neo4j driver will try to resolve DNS hostname 
    config = {
        "scheme": "bolt", "host": "google.com", "port": 80, "username": "myusername", "password": "mypassword",
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"slices\", \"slices_count_per_incremental_sync\": 10, \"max_records_per_incremental_sync\": 50}}"
    }
    return config


def test_check_connection_success(mocker, config_mock):
    mocker.patch.object(Neo4jClient, "verify_connectivity", return_value="myvalue")
    source = SourceNeo4j()
    logger_mock = MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (True, None)


def test_check_connection_failure(mocker, config_mock):
    mocker.patch.object(Neo4jClient, "verify_connectivity", side_effect=Exception("error"))
    source = SourceNeo4j()
    logger_mock = MagicMock()
    
    assert isinstance(source.check_connection(logger_mock, config_mock), tuple)
    assert source.check_connection(logger_mock, config_mock)[0] == False
    assert isinstance(source.check_connection(logger_mock, config_mock)[1], str)


def test_streams_empty(mocker, config_mock):
    mocker.patch.object(Neo4jClient, "node_labels", new_callable=PropertyMock, return_value =[])
    mocker.patch.object(Neo4jClient, "relationship_types", new_callable=PropertyMock, return_value =[])
    source = SourceNeo4j()
    
    streams = source.streams(config_mock)
    
    assert isinstance(streams, list)
    assert len(streams) == 0


def test_streams_nodes_only(mocker, config_mock):
    mocker.patch.object(Neo4jClient, "node_labels", new_callable=PropertyMock, return_value =["node_1", "node_2", "node_3"])
    mocker.patch.object(Neo4jClient, "node_json_schemas", new_callable=PropertyMock, return_value ={"node_1": {}, "node_2": {}, "node_3": {}})
    mocker.patch.object(Neo4jClient, "relationship_types", new_callable=PropertyMock, return_value =[])
    source = SourceNeo4j()
    
    streams = source.streams(config_mock)
    
    assert isinstance(streams, list)
    assert len(streams) == 3
    assert all([isinstance(stream, NodeStream) for stream in streams])


def test_streams_relationships_only(mocker, config_mock):
    mocker.patch.object(Neo4jClient, "node_labels", new_callable=PropertyMock, return_value =[])
    mocker.patch.object(Neo4jClient, "relationship_types", new_callable=PropertyMock, return_value =["rel_1", "rel_2"])
    mocker.patch.object(Neo4jClient, "relationship_json_schemas", new_callable=PropertyMock, return_value ={"rel_1": {}, "rel_2": {}})
    source = SourceNeo4j()
    
    streams = source.streams(config_mock)
    
    assert isinstance(streams, list)
    assert len(streams) == 2
    assert all([isinstance(stream, RelationshipStream) for stream in streams])


def test_streams_nodes_and_relationships(mocker, config_mock):
    mocker.patch.object(Neo4jClient, "node_labels", new_callable=PropertyMock, return_value =["node_1", "node_2", "node_3"])
    mocker.patch.object(Neo4jClient, "node_json_schemas", new_callable=PropertyMock, return_value ={"node_1": {}, "node_2": {}, "node_3": {}})
    mocker.patch.object(Neo4jClient, "relationship_types", new_callable=PropertyMock, return_value =["rel_1", "rel_2"])
    mocker.patch.object(Neo4jClient, "relationship_json_schemas", new_callable=PropertyMock, return_value ={"rel_1": {}, "rel_2": {}})
    source = SourceNeo4j()
    
    streams = source.streams(config_mock)
    
    assert isinstance(streams, list)
    assert len(streams) == 5
    assert all([isinstance(stream, IncrementalNeo4jStream) for stream in streams])
