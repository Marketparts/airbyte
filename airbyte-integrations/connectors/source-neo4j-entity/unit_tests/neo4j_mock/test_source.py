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


from source_neo4j_entity.source import SourceNeo4jEntity
from unittest.mock import MagicMock, PropertyMock

from source_neo4j_entity.neo4j import Neo4jClient
from source_neo4j_entity.streams import IncrementalNeo4jEntityStream, NodeStream, RelationshipStream



def test_check_connection_success(mocker):
    mocker.patch.object(Neo4jClient, "verify_connectivity", return_value="myvalue")
    source = SourceNeo4jEntity()
    config_mock, logger_mock = MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (True, None)


def test_check_connection_failure(mocker):
    mocker.patch.object(Neo4jClient, "verify_connectivity", side_effect=Exception("error"))
    source = SourceNeo4jEntity()
    config_mock, logger_mock = MagicMock(), MagicMock()
    
    assert isinstance(source.check_connection(logger_mock, config_mock), tuple)
    assert source.check_connection(logger_mock, config_mock)[0] == False
    assert isinstance(source.check_connection(logger_mock, config_mock)[1], str)


def test_streams_empty(mocker):
    mocker.patch.object(Neo4jClient, "node_labels", new_callable=PropertyMock, return_value =[])
    mocker.patch.object(Neo4jClient, "relationship_types", new_callable=PropertyMock, return_value =[])
    source = SourceNeo4jEntity()
    config_mock = {}
    streams = source.streams(config_mock)
    
    assert isinstance(streams, list)
    assert len(streams) == 0


def test_streams_nodes_only(mocker):
    mocker.patch.object(Neo4jClient, "node_labels", new_callable=PropertyMock, return_value =["node_1", "node_2", "node_3"])
    mocker.patch.object(Neo4jClient, "node_json_schemas", new_callable=PropertyMock, return_value ={"node_1": {}, "node_2": {}, "node_3": {}})
    mocker.patch.object(Neo4jClient, "relationship_types", new_callable=PropertyMock, return_value =[])
    source = SourceNeo4jEntity()
    config_mock = {}
    streams = source.streams(config_mock)
    
    assert isinstance(streams, list)
    assert len(streams) == 3
    assert all([isinstance(stream, NodeStream) for stream in streams])


def test_streams_relationships_only(mocker):
    mocker.patch.object(Neo4jClient, "node_labels", new_callable=PropertyMock, return_value =[])
    mocker.patch.object(Neo4jClient, "relationship_types", new_callable=PropertyMock, return_value =["rel_1", "rel_2"])
    mocker.patch.object(Neo4jClient, "relationship_json_schemas", new_callable=PropertyMock, return_value ={"rel_1": {}, "rel_2": {}})
    source = SourceNeo4jEntity()
    config_mock = {}
    streams = source.streams(config_mock)
    
    assert isinstance(streams, list)
    assert len(streams) == 2
    assert all([isinstance(stream, RelationshipStream) for stream in streams])


def test_streams_nodes_and_relationships(mocker):
    mocker.patch.object(Neo4jClient, "node_labels", new_callable=PropertyMock, return_value =["node_1", "node_2", "node_3"])
    mocker.patch.object(Neo4jClient, "node_json_schemas", new_callable=PropertyMock, return_value ={"node_1": {}, "node_2": {}, "node_3": {}})
    mocker.patch.object(Neo4jClient, "relationship_types", new_callable=PropertyMock, return_value =["rel_1", "rel_2"])
    mocker.patch.object(Neo4jClient, "relationship_json_schemas", new_callable=PropertyMock, return_value ={"rel_1": {}, "rel_2": {}})
    source = SourceNeo4jEntity()
    config_mock = {}
    streams = source.streams(config_mock)
    
    assert isinstance(streams, list)
    assert len(streams) == 5
    assert all([isinstance(stream, IncrementalNeo4jEntityStream) for stream in streams])
