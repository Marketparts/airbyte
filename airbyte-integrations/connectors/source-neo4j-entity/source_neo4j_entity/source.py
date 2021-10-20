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

from typing import Any, List, Mapping, Tuple


from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream



from source_neo4j_entity.streams import Neo4jClient
from source_neo4j_entity.streams import NodeStream, RelationshipStream


class SourceNeo4jEntity(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Check if connection to Neo4j database is available with the configuration provided

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        check_result = (False, "")

        try:
            # check connectivity to the neo4j database
            client = Neo4jClient(config)
            client.verify_connectivity()

            check_result = (True, None)

        except Exception as e:
            logger.error("Unable to connect to neo4j on bolt://{}:{}, reason: {}".format(config['host'], config['port'], str(e)))
            check_result[1] = e


        return check_result


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Get all entities (nodes or relationships) and exposes them as streams

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        client = Neo4jClient(config=config)

        streams = []
        for label in client.node_labels:
            streams.append(NodeStream(label=label, client=client, config=config))

        
        for type in client.relationship_types:
            streams.append(RelationshipStream(type=type, client=client, config=config))

        return streams

