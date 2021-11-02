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


from source_neo4j_entity.source import SourceNeo4jEntity
from unittest.mock import MagicMock

from source_neo4j_entity.streams import IncrementalNeo4jEntityStream, NodeStream, RelationshipStream


def test_check_connection_success(neo4j_container):
    source = SourceNeo4jEntity()
    logger_mock = MagicMock()

    assert source.check_connection(logger_mock, pytest.neo4j_client_config) == (True, None)


def test_check_connection_failure(neo4j_container):
    source = SourceNeo4jEntity()
    logger_mock = MagicMock()

    config = pytest.neo4j_client_config.copy()
    config["password"] = "wrongpass"

    assert isinstance(source.check_connection(logger_mock, config), tuple)
    assert source.check_connection(logger_mock, config)[0] == False
    assert isinstance(source.check_connection(logger_mock, config)[1], str)


def test_streams(neo4j_container):
    source = SourceNeo4jEntity()
    
    streams = source.streams(pytest.neo4j_client_config)

    assert isinstance(streams, list)
    assert len(streams) == 4
    assert all([isinstance(stream, IncrementalNeo4jEntityStream) for stream in streams])
