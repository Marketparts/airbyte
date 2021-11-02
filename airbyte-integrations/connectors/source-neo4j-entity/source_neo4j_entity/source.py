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

# TODO: remove when overriding _read_incremental() is no longer required
from typing import Iterator, MutableMapping
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteMessage,
    ConfiguredAirbyteStream,
    SyncMode,
)
################


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
            error = "Unable to connect to neo4j on {}, reason: {}".format(client.uri, str(e))
            logger.error(error)
            check_result = (False, error)

        return check_result


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Get all entities (nodes or relationships) and exposes them as streams

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        client = Neo4jClient(config=config, preload_schemas=True)
        
        streams = []
        for label in client.node_labels:
            streams.append(NodeStream(label=label, client=client, config=config))

        
        for type in client.relationship_types:
            streams.append(RelationshipStream(type=type, client=client, config=config))

        return streams


    def _read_incremental(
        self,
        logger: AirbyteLogger,
        stream_instance: Stream,
        configured_stream: ConfiguredAirbyteStream,
        connector_state: MutableMapping[str, Any],
        internal_config: InternalConfig,
    ) -> Iterator[AirbyteMessage]:
        """
        Overrides AbstractSource method due to issue https://github.com/airbytehq/airbyte/issues/7390 
        """
        stream_name = configured_stream.stream.name
        stream_state = connector_state.get(stream_name, {})
        if stream_state:
            logger.info(f"Setting state of {stream_name} stream to {stream_state}")

        checkpoint_interval = stream_instance.state_checkpoint_interval
        slices = stream_instance.stream_slices(
            cursor_field=configured_stream.cursor_field, sync_mode=SyncMode.incremental, stream_state=stream_state
        )
        total_records_counter = 0
        for slice in slices:
            records = stream_instance.read_records(
                sync_mode=SyncMode.incremental,
                stream_slice=slice,
                stream_state=stream_state,
                cursor_field=configured_stream.cursor_field or None,
            )
            for record_counter, record_data in enumerate(records, start=1):
                yield self._as_airbyte_record(stream_name, record_data)
                stream_state = stream_instance.get_updated_state(stream_state, record_data, configured_stream.cursor_field)
                if checkpoint_interval and record_counter % checkpoint_interval == 0:
                    yield self._checkpoint_state(stream_name, stream_state, connector_state, logger)

                total_records_counter += 1
                # This functionality should ideally live outside of this method
                # but since state is managed inside this method, we keep track
                # of it here.
                if self._limit_reached(internal_config, total_records_counter):
                    # Break from slice loop to save state and exit from _read_incremental function.
                    break

            yield self._checkpoint_state(stream_name, stream_state, connector_state, logger)
            if self._limit_reached(internal_config, total_records_counter):
                return
