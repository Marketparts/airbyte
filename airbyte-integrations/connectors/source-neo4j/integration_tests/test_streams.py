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
import json

from typing import Iterable
import pytest

from airbyte_cdk.models import SyncMode
from source_neo4j.neo4j import Neo4jClient
from source_neo4j.streams import NodeStream, RelationshipStream, CypherStream


@pytest.fixture
def stream_config():
    config = {
        "enable_dynamic_schemas": True,
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"slices\", \"slices_count_per_incremental_sync\": 10}}"
    }

    return config


def test_describe_slices(neo4j_container, stream_config):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)
    stream = NodeStream(label="node_1", client=client, config=stream_config)

    expected = {
       "count": 100,
        "min": [1633103553000],
        "max": [1633107810000]
    }

    assert stream._describe_slices(cursor_field=["updated_at"]) == expected

    # test relationship stream
    stream = RelationshipStream(type="rel_1", client=client, config=stream_config)

    # as the cursor field is _identity, its value is internally managed by Neo4j and we cannot now the range of ids
    expected = {
        "count": 50
    }

    assert stream._describe_slices(cursor_field="_identity")["count"] == expected["count"]

    assert isinstance(stream._describe_slices(cursor_field="_identity")["min"], list)
    assert all(isinstance(x, int) for x in stream._describe_slices(cursor_field="_identity")["min"])
    assert isinstance(stream._describe_slices(cursor_field="_identity")["max"], list)
    assert all(isinstance(x, int) for x in stream._describe_slices(cursor_field="_identity")["max"])



def test_get_cursor_value_for_percentiles(neo4j_container, stream_config):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    stream = NodeStream(label="node_1", client=client, config=stream_config)

    expected = {
        0.1: [1633103940000],
        0.2: [1633104370000]
    }

    assert stream._get_cursor_value_for_percentiles(
        percentiles=[0.1,0.2],
        cursor_field=["updated_at"],
        cursor_min=[1633103553000],
        cursor_max=[1633107810000]
    ) == expected


def test_stream_slices(neo4j_container, stream_config):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    stream = NodeStream(label="node_1", client=client, config=stream_config)
    
    inputs = {"sync_mode": SyncMode.incremental, "cursor_field": ["updated_at"], "stream_state": {}}
    
    expected_stream_slice = [
        {'from': [1633103552999], 'to': [1633103940000]},
        {'from': [1633103940000], 'to': [1633104370000]},
        {'from': [1633104370000], 'to': [1633104800000]},
        {'from': [1633104800000], 'to': [1633105230000]},
        {'from': [1633105230000], 'to': [1633105660000]},
        {'from': [1633105660000], 'to': [1633106090000]},
        {'from': [1633106090000], 'to': [1633106520000]},
        {'from': [1633106520000], 'to': [1633106950000]},
        {'from': [1633106950000], 'to': [1633107380000]},
        {'from': [1633107380000], 'to': [1633107810000]}
    ]
    
    assert stream.stream_slices(**inputs) == expected_stream_slice


def test_stream_slices_for_composite_cursor(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    ######################################
    # Test interval checkpointing mode : one slice
    ######################################
    stream_config = {
        "enable_dynamic_schemas": False,
        "custom_streams": "{\"custom1\": {\"match_query\": \"MATCH (A:node_2)-[:rel_1]->(B:node_3)\", \"return_query\": \"RETURN A.primary_key, A.updated_at, B.primary_key, B.updated_at\", \"cursor_field\": [\"A.updated_at\", \"B.updated_at\"]}}",
        "json_schemas": "{\"custom1\": {\"A.updated_at\":{\"type\":\"integer\"}, \"B.updated_at\":{\"type\":\"integer\"}}}",
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"interval\", \"state_checkpoint_interval\": 20, \"max_records_per_incremental_sync\": 100}}"
    }

    stream = CypherStream(name="custom1", client=client, config=stream_config)
    
    stream_config_custom_streams = json.loads(stream_config["custom_streams"])
    inputs = {"sync_mode": SyncMode.incremental, "cursor_field": stream_config_custom_streams["custom1"]["cursor_field"], "stream_state": {}}
    
    expected_stream_slice = [{'from': [1633103552999, 1633103552999], 'to': [1633107786600, 1633105660000]}]
    assert stream.stream_slices(**inputs) == expected_stream_slice


    ######################################
    # Test slice checkpointing mode : 5 slices
    ######################################
    stream_config = {
        "enable_dynamic_schemas": False,
        "custom_streams": "{\"custom1\": {\"match_query\": \"MATCH (A:node_2)-[:rel_1]->(B:node_3)\", \"return_query\": \"RETURN A.primary_key, A.updated_at, B.primary_key, B.updated_at\", \"cursor_field\": [\"A.updated_at\", \"B.updated_at\"]}}",
        "json_schemas": "{\"custom1\": {\"A.updated_at\":{\"type\":\"integer\"}, \"B.updated_at\":{\"type\":\"integer\"}}}",
        "incremental_sync_settings": "{\"DEFAULT\": {\"checkpointing_mode\": \"slices\", \"slices_count_per_incremental_sync\": 5, \"max_records_per_incremental_sync\": 100}}"
    }

    stream = CypherStream(name="custom1", client=client, config=stream_config)
    
    stream_config_custom_streams = json.loads(stream_config["custom_streams"])
    inputs = {"sync_mode": SyncMode.incremental, "cursor_field": stream_config_custom_streams["custom1"]["cursor_field"], "stream_state": {}}
    
    expected_stream_slice = [
        {'from': [1633103552999, 1633103552999], 'to': [1633104330600, 1633103940000]},
        {'from': [1633104330600, 1633103940000], 'to': [1633105194600, 1633104370000]},
        {'from': [1633105194600, 1633104370000], 'to': [1633106058600, 1633104800000]},
        {'from': [1633106058600, 1633104800000], 'to': [1633106922600, 1633105230000]},
        {'from': [1633106922600, 1633105230000], 'to': [1633107786600, 1633105660000]},
    ]
    assert stream.stream_slices(**inputs) == expected_stream_slice



def test_stream_slices_greater_state(neo4j_container, stream_config):
    """
    Test when state is greater than max of cursor 
    """
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    stream = NodeStream(label="node_1", client=client, config=stream_config)
    
    inputs = {"sync_mode": SyncMode.incremental, "cursor_field": ["updated_at"], "stream_state": {"updated_at": 2000000000000}}
    
    expected_stream_slice = [
        {'from': [2000000000000], 'to': [2000000000000]}
    ]
    
    assert stream.stream_slices(**inputs) == expected_stream_slice


def test_read_records_greater_state(neo4j_container, stream_config):
    """
    Test no record returned when state is greater than max of cursor 
    """
    client = Neo4jClient(config=pytest.neo4j_client_config, clear_cache=True)

    stream = NodeStream(label="node_1", client=client, config=stream_config)
    
    inputs = {
        "sync_mode": SyncMode.incremental,
        "cursor_field": ["updated_at"],
        "stream_slice": {'from': [2000000000000], 'to': [2000000000000]},
        "stream_state": {"updated_at": 2000000000000}
    }
    
    records = stream.read_records(**inputs)
    assert isinstance(records, Iterable)
    assert len([record for record in records]) == 0
