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
from unittest.mock import MagicMock

from airbyte_cdk.models import SyncMode
from source_neo4j_entity.neo4j import Neo4jClient
from source_neo4j_entity.streams import IncrementalNeo4jEntityStream

@pytest.fixture
def mock_neo4j_client():
    # use a real hostname as neo4j driver will try to resolve DNS hostname 
    config = {"scheme": "bolt", "host": "google.com", "port": 80, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)
    

@pytest.fixture
def mock_incremental_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(IncrementalNeo4jEntityStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalNeo4jEntityStream, "__abstractmethods__", set())


def test_set_config_success(mock_neo4j_client, mock_incremental_base_class):
    config = {
        "state_checkpoint_interval": 10,
        "max_records_per_incremental_sync": 100,
        "slices_count_per_incremental_sync": 10 
    }

    stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client, config=config)

    assert hasattr(stream, "_state_checkpoint_interval")
    assert hasattr(stream, "_max_records_per_incremental_sync")
    assert hasattr(stream, "_slices_count_per_incremental_sync")

    assert stream._state_checkpoint_interval == config["state_checkpoint_interval"]
    assert stream._max_records_per_incremental_sync == config["max_records_per_incremental_sync"]
    assert stream._slices_count_per_incremental_sync == config["slices_count_per_incremental_sync"]

    assert stream.state_checkpoint_interval == config["state_checkpoint_interval"]


def test_set_config_failure(mock_neo4j_client, mock_incremental_base_class):
    config = {
        "state_checkpoint_interval": 10,
        "max_records_per_incremental_sync": 100,
        "slices_count_per_incremental_sync": 10 
    }

    config = {"state_checkpoint_interval": "test"}
    with pytest.raises(ValueError) as e:
        stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client, config=config)

    config = {"max_records_per_incremental_sync": "test"}
    with pytest.raises(ValueError) as e:
        stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client, config=config)

    config = {"slices_count_per_incremental_sync": "test"}
    with pytest.raises(ValueError) as e:
        stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client, config=config)


def test_state_checkpoint_interval(mock_neo4j_client, mock_incremental_base_class):
    stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client)
    assert stream.state_checkpoint_interval == 1000

    config = {"state_checkpoint_interval": 10}
    stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client, config=config)
    assert stream.state_checkpoint_interval == config["state_checkpoint_interval"]


def test_source_defined_cursor(mock_neo4j_client, mock_incremental_base_class):
    stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client)
    assert not stream.source_defined_cursor


def test_cursor_field(mock_neo4j_client, mock_incremental_base_class):
    stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client)
    expected_cursor_field = []
    assert stream.cursor_field == expected_cursor_field


def test_get_updated_state(mock_neo4j_client, mock_incremental_base_class):
    stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client)
    inputs = {"current_stream_state": None, "latest_record": {"id": 3, "updated_at": 1635361752}, "cursor_field": "updated_at"}

    expected_state = {"updated_at": 1635361752}
    assert stream.get_updated_state(**inputs) == expected_state


def test_supports_incremental(mock_neo4j_client, mock_incremental_base_class):
    stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client)
    assert stream.supports_incremental


def test_get_cypher_identifier_for_cursor(mock_neo4j_client, mock_incremental_base_class):
    stream = IncrementalNeo4jEntityStream(client=mock_neo4j_client)

    cursor_field = "_identity"
    expected = "ID(incremental_neo4j_entity_stream)"
    assert stream._get_cypher_identifier_for_cursor(cursor_field) == expected

    cursor_field = "Other"
    expected = "incremental_neo4j_entity_stream.Other"
    assert stream._get_cypher_identifier_for_cursor(cursor_field) == expected

def test_describe_slices(mock_incremental_base_class, mocker):
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter([[4, 1234, 5678]]))
    config = {"scheme": "bolt", "host": "google.com", "port": 80, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)
    stream = IncrementalNeo4jEntityStream(client=client)

    expected = {
        "records_count": 4,
        "cursor_min": 1234,
        "cursor_max": 5678
    }

    assert stream._describe_slices(query_cursor="cursor") == expected


def test_get_cursor_value_for_percentiles(mock_incremental_base_class, mocker):
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter([{"percentile": 0.1, "cursor_value": 1234}, {"percentile": 0.2, "cursor_value": 5678}]))
    config = {"scheme": "bolt", "host": "google.com", "port": 80, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)
    stream = IncrementalNeo4jEntityStream(client=client)

    expected = {
        0.1: 1234,
        0.2: 5678
    }

    assert stream._get_cursor_value_for_percentiles(percentiles=[0.1,0.2], query_cursor="cursor", cursor_min=10, cursor_max=10000) == expected
        