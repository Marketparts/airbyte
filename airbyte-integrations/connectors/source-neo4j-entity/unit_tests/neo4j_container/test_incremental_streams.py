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

from airbyte_cdk.models import SyncMode
from source_neo4j_entity.neo4j import Neo4jClient
from source_neo4j_entity.streams import NodeStream



def test_describe_slices(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config)
    stream = NodeStream(label="node_1", client=client)

    expected = {
        "records_count": 100,
        "cursor_min": 1633103553000,
        "cursor_max": 1633107810000
    }

    query_cursor = stream._get_cypher_identifier_for_cursor("updated_at")

    assert stream._describe_slices(query_cursor=query_cursor) == expected


def test_get_cursor_value_for_percentiles(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config)

    stream = NodeStream(label="node_1", client=client)

    query_cursor = stream._get_cypher_identifier_for_cursor("updated_at")

    expected = {
        0.1: 1633103940000,
        0.2: 1633104370000
    }

    assert stream._get_cursor_value_for_percentiles(
        percentiles=[0.1,0.2],
        query_cursor=query_cursor,
        cursor_min=1633103553000,
        cursor_max=1633107810000
    ) == expected


def test_stream_slices(neo4j_container):
    client = Neo4jClient(config=pytest.neo4j_client_config)

    stream_config = {
        "max_records_per_incremental_sync": 1000000,
        "slices_count_per_incremental_sync": 10 
    }

    stream = NodeStream(label="node_1", client=client, config=stream_config)
    
    inputs = {"sync_mode": SyncMode.incremental, "cursor_field": ["updated_at"], "stream_state": {}}
    
    expected_stream_slice = [
        {'from': 1633103552999, 'to': 1633103940000},
        {'from': 1633103940000, 'to': 1633104370000},
        {'from': 1633104370000, 'to': 1633104800000},
        {'from': 1633104800000, 'to': 1633105230000},
        {'from': 1633105230000, 'to': 1633105660000},
        {'from': 1633105660000, 'to': 1633106090000},
        {'from': 1633106090000, 'to': 1633106520000},
        {'from': 1633106520000, 'to': 1633106950000},
        {'from': 1633106950000, 'to': 1633107380000},
        {'from': 1633107380000, 'to': 1633107810000}
    ]
    
    assert stream.stream_slices(**inputs) == expected_stream_slice


