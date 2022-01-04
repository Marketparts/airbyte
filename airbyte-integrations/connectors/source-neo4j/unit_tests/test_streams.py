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
import json
from unittest.mock import MagicMock

from airbyte_cdk.models import SyncMode
from source_neo4j.neo4j import Neo4jClient
from source_neo4j.streams import IncrementalNeo4jStream, NodeStream

@pytest.fixture
def mock_neo4j_client():
    # use a real hostname as neo4j driver will try to resolve DNS hostname 
    config = {"scheme": "bolt", "host": "google.com", "port": 80, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)
    

@pytest.fixture
def mock_incremental_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(IncrementalNeo4jStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalNeo4jStream, "__abstractmethods__", set())

@pytest.fixture
def stream_config():
    config = {
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"slices\", \"slices_count_per_incremental_sync\": 10}}"
    }

    return config


def test_set_config_success(mock_neo4j_client, mock_incremental_base_class):
    config = {
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"slices\", \"slices_count_per_incremental_sync\": 10, \"max_records_per_incremental_sync\": 100}}"
    }

    stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=config)

    assert hasattr(stream, "_checkpointing_mode")
    assert hasattr(stream, "_max_records_per_incremental_sync")
    assert hasattr(stream, "_slices_count_per_incremental_sync")
    
    config["incremental_sync_settings"] = json.loads(config["incremental_sync_settings"])
    assert stream._checkpointing_mode == config["incremental_sync_settings"]["DEFAULT"]["checkpointing_mode"]
    assert stream._max_records_per_incremental_sync == config["incremental_sync_settings"]["DEFAULT"]["max_records_per_incremental_sync"]
    assert stream._slices_count_per_incremental_sync == config["incremental_sync_settings"]["DEFAULT"]["slices_count_per_incremental_sync"]



def test_set_config_failure(mock_neo4j_client, mock_incremental_base_class):
    config = {
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"slices\", \"slices_count_per_incremental_sync\": \"test\"}}"
    }

    with pytest.raises(ValueError) as e:
        stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=config)

    config = {
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"slices\", \"slices_count_per_incremental_sync\": 10, \"max_records_per_incremental_sync\": \"test\"}}"
    }
    with pytest.raises(ValueError) as e:
        stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=config)


    config = {
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"interval\", \"state_checkpoint_interval\": \"test\"}}"
    }
    with pytest.raises(ValueError) as e:
        stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=config)


def test_state_checkpoint_interval(mock_neo4j_client, mock_incremental_base_class):
    config = {
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"interval\", \"state_checkpoint_interval\": 10}}"
    }
    stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=config)
    config["incremental_sync_settings"] = json.loads(config["incremental_sync_settings"])
    assert stream.state_checkpoint_interval == config["incremental_sync_settings"]["DEFAULT"]["state_checkpoint_interval"]


def test_source_defined_cursor(mock_neo4j_client, mock_incremental_base_class, stream_config):
    stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=stream_config)
    assert not stream.source_defined_cursor


def test_cursor_field(mock_neo4j_client, mock_incremental_base_class, stream_config):
    stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=stream_config)
    expected_cursor_field = []
    assert stream.cursor_field == expected_cursor_field


def test_get_updated_state(mock_neo4j_client, mock_incremental_base_class, stream_config):
    stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=stream_config)
    inputs = {"current_stream_state": None, "latest_record": {"id": 3, "updated_at": 1635361752}, "cursor_field": ["updated_at"]}

    expected_state = {"updated_at": 1635361752}
    assert stream.get_updated_state(**inputs) == expected_state


def test_supports_incremental(mock_neo4j_client, mock_incremental_base_class, stream_config):
    stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=stream_config)
    assert stream.supports_incremental


def test_get_cypher_identifier_for_cursor(mock_neo4j_client, mock_incremental_base_class, stream_config):
    stream = IncrementalNeo4jStream(client=mock_neo4j_client, config=stream_config)

    cursor_field = "_identity"
    expected = "ID(incremental_neo4j_stream)"
    assert stream._get_cypher_identifier_for_cursor(cursor_field) == expected

    cursor_field = "Other"
    expected = "incremental_neo4j_stream.Other"
    assert stream._get_cypher_identifier_for_cursor(cursor_field) == expected

def test_describe_slices(mock_incremental_base_class, mocker, stream_config):
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter([[4, [1234, 5678]]]))
    config = {"scheme": "bolt", "host": "google.com", "port": 80, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)
    stream = IncrementalNeo4jStream(client=client, config=stream_config)

    expected = {
        "count": 4,
        "min": [1234],
        "max": [5678]
    }

    assert stream._describe_slices(cursor_field=["cursor"]) == expected


def test_get_cursor_value_for_percentiles(mock_incremental_base_class, mocker, stream_config):
    mocker.patch.object(Neo4jClient, "fetch_results", return_value=iter([{"value": {"percentile": 0.1, "cursor_value_0": 1234}}, {"value": {"percentile": 0.2, "cursor_value_0": 5678}}]))
    config = {"scheme": "bolt", "host": "google.com", "port": 80, "username": "myusername", "password": "mypassword"}
    client = Neo4jClient(config=config)
    stream = IncrementalNeo4jStream(client=client, config=stream_config)

    expected = {
        0.1: [1234],
        0.2: [5678]
    }

    assert stream._get_cursor_value_for_percentiles(percentiles=[0.1,0.2], cursor_field=["cursor"], cursor_min=[10], cursor_max=[10000]) == expected
