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

import os
import time

import pytest

from docker_neo4j.neo4j_container import Neo4jTestInstance



@pytest.fixture(scope="session", autouse=True)
def neo4j_container():
    """ This fixture is a placeholder for external resources that acceptance test might require."""

    # lauch docker compose to get a neo4j container
    try:
        neo4j = Neo4jTestInstance(docker_compose_filepath=os.path.dirname(__file__))

        neo4j.start()
        neo4j.populate_test_data()

        pytest.neo4j_client_config = neo4j.config
        
    except Exception as e:
        # stop the container before raising the exception
        neo4j.stop()

        # wait 10s to let the container stop
        time.sleep(10)

        raise e

    yield
    neo4j.stop()

    
