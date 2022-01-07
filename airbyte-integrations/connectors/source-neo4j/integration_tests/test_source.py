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
import os
import json

from airbyte_cdk.models import Type
from source_neo4j.source import SourceNeo4j


@pytest.fixture
def assert_read_records_are_expected():
    def assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename, state_filename=None):
        integration_tests_dirpath = os.path.dirname(os.path.realpath(__file__))
        config_filepath = os.path.sep.join([integration_tests_dirpath, "config", config_filename])
        catalog_filepath = os.path.sep.join([integration_tests_dirpath, "configured_catalog", catalog_filename])
        expected_results_filepath = os.path.sep.join([integration_tests_dirpath, "expected_results", expected_results_filename])
        state_filepath = os.path.sep.join([integration_tests_dirpath, "state", state_filename]) if state_filename is not None else None

        source = SourceNeo4j()
        
        config = source.read_config(config_filepath)
        configured_catalog = source.read_catalog(catalog_filepath)
        state = source.read_state(state_filepath) if state_filepath is not None else None
        logger = source.logger

        actual_messages = source.read(logger=logger, config=config, catalog=configured_catalog, state=state)

        with open(expected_results_filepath) as file:
            expected_messages = file.read().splitlines()


        actual_messages_json = []
        for mess in actual_messages:
            mess_json = json.loads(mess.json(exclude_none=True))
            if mess.type == Type.RECORD:
                
                # remove "emitted_at" attribute for comparison because it changes overtime
                if mess_json["record"].get("emitted_at") is not None:
                    del mess_json["record"]["emitted_at"]
                
            actual_messages_json.append(mess_json)
        

        expected_messages_json = []
        for row in expected_messages:
            expected_mess = json.loads(row)

            # remove "emitted_at" attribute for comparison because it changes overtime
            if expected_mess.get("record") is not None:
                if expected_mess["record"].get("emitted_at") is not None:
                    del expected_mess["record"]["emitted_at"]
            
            expected_messages_json.append(expected_mess)

        for i, message in enumerate(actual_messages_json):
            assert json.dumps(actual_messages_json[i], sort_keys=True)  == json.dumps(expected_messages_json[i], sort_keys=True)
        
    return assert_read_records_are_expected


def test_read_records_full_refresh_node(assert_read_records_are_expected):
    config_filename = "config_node_or_relationship_full_refresh.json"
    catalog_filename = "configured_catalog_full_refresh_node.json"
    expected_results_filename = "expected_messages_node_full_refresh.txt"
    
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename)


def test_read_records_full_refresh_relationship(assert_read_records_are_expected):
    config_filename = "config_node_or_relationship_full_refresh.json"
    catalog_filename = "configured_catalog_full_refresh_relationship.json"
    expected_results_filename = "expected_messages_relationship_full_refresh.txt"
    
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename)


def test_read_records_full_refresh_cypher(assert_read_records_are_expected):
    config_filename = "config_cypher_full_refresh.json"
    catalog_filename = "configured_catalog_full_refresh_cypher.json"
    expected_results_filename = "expected_messages_cypher_full_refresh.txt"
    
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename)


def test_read_records_incremental_node_simple_cursor_interval(assert_read_records_are_expected):
    config_filename = "config_all_simple_cursor_interval.json"
    catalog_filename = "configured_catalog_incremental_node_simple_cursor.json"
    
    # test with no state
    expected_results_filename = "expected_messages_node_simple_cursor_incremental_interval.txt"
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename)

    # test with state
    state_filename = "state_simple_cursor_node_1.json"
    expected_results_filename = "expected_messages_node_simple_cursor_incremental_interval_with_state.txt"
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename, state_filename)


def test_read_records_incremental_cypher_composite_cursor_interval(assert_read_records_are_expected):
    config_filename = "config_cypher_composite_cursor_interval.json"
    catalog_filename = "configured_catalog_incremental_cypher_composite_cursor.json"
    expected_results_filename = "expected_messages_cypher_composite_cursor_incremental_interval.txt"
    
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename)



def test_read_records_incremental_cypher_composite_cursor_interval_max_records_per_sync(assert_read_records_are_expected):
    config_filename = "config_cypher_composite_cursor_interval_max_records_per_sync.json"
    catalog_filename = "configured_catalog_incremental_cypher_composite_cursor.json"
    expected_results_filename = "expected_messages_cypher_composite_cursor_incremental_interval_max_records_per_sync.txt"
    
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename)


def test_read_records_incremental_cypher_composite_cursor_slices_max_records_per_slice_greater_than_total(assert_read_records_are_expected):
    config_filename = "config_cypher_composite_cursor_slices_max_records_per_slice_greater_than_total.json"
    catalog_filename = "configured_catalog_incremental_cypher_composite_cursor.json"

    # test with no state
    expected_results_filename = "expected_messages_cypher_composite_cursor_incremental_slices_max_records_per_slice_greater_than_total.txt"
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename)

    # test with state
    state_filename = "state_composite_cursor.json"
    expected_results_filename = "expected_messages_cypher_composite_cursor_incremental_slices_max_records_per_slice_greater_than_total_with_state.txt"
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename, state_filename)


def test_read_records_incremental_cypher_composite_cursor_slices_max_records_per_slice_and_sync(assert_read_records_are_expected):
    config_filename = "config_cypher_composite_cursor_slices_max_records_per_slice_and_sync.json"
    catalog_filename = "configured_catalog_incremental_cypher_composite_cursor.json"
    expected_results_filename = "expected_messages_cypher_composite_cursor_incremental_slices_max_records_per_slice_and_sync.txt"
    
    assert_read_records_are_expected(config_filename, catalog_filename, expected_results_filename)