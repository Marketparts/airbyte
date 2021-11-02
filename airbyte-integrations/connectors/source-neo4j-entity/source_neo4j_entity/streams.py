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

import inspect, math
from abc import ABC, abstractmethod, abstractproperty
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader

from neo4j import graph, data
from pydantic.errors import IntegerError

from source_neo4j_entity.neo4j import Neo4jClient


def package_name_from_class(cls: object) -> str:
    """Find the package name given a class name"""
    module: Any = inspect.getmodule(cls)
    return module.__name__.split(".")[0]




class Neo4jEntityStream(Stream, ABC):
    """
    Neo4j stream mapped to entities (nodes or relationships)
    """
    def __init__(self, client: Neo4jClient, config: Mapping[str, Any] = None) -> None:
        self._client = client
        config = config or {}

        self._set_config(config)

    @abstractmethod
    def _set_config(self, config: Mapping[str, Any]) -> None:
        """
        Set stream config parameters
        """

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        :return: A dict of the JSON schema representing this stream.

        This method looks for a JSONSchema file with the same name as this stream's "_entity_type" property.
        Ovverides Stream.get_json_schema()
        """
        # TODO investigate how to construct the full schema
        # option 1: fast but approximative: all properties in the database for each entity in the database
        # option 2: slow (db scan) but more accurate: real properties for each entity in the database
        
        base_schema = ResourceSchemaLoader(package_name_from_class(self.__class__)).get_schema(self.entity_type)
        
        # option2: we add the list of all properties existing in the database whatever the entity it belongs to
        # property_keys = self._client.property_keys
        # for prop in property_keys:
        #     # as the type of neo4j properties are not constraint, it can be of every type
        #     base_schema["properties"][prop] = {"type": [
        #         "object", "boolean", "string", "number", "array", "null"
        #     ]}

        # option3: we add the list of real properties for each entity in the database
        if self.entity_type == "node":
            base_schema["properties"] = {**base_schema["properties"], **self._client.node_json_schemas[self.name]}
        elif self.entity_type == "relationship":
            base_schema["properties"] = {**base_schema["properties"], **self._client.relationship_json_schemas[self.name]}
        else:
            raise ValueError("Entity type {} not supported".format(self.entity_type))


        return base_schema


    @abstractproperty
    def entity_type(self) -> str:
        """
        :return: Neo4j entity type (node, relationship)
        """

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """
        :return: string if single primary key, list of strings if composite primary key, list of list of strings if composite primary key consisting of nested fields.
        If the stream has no primary keys, return None.
        """
        return None


    @abstractmethod
    def _get_cypher_query(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Any:
        """
        Get cypher query and optional parameters for fetching entity
        :return: string cypher query or dict with "query" and "params" keys
        """

    @staticmethod
    @abstractmethod
    def _record_to_dict(self, record) -> Mapping[str, Any]:
        """
        Transform a record fetched by the python driver to dict
        :return: dict of the record object
        """        

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        This method should be overridden by subclasses to read records based on the inputs
        """
        query = self._get_cypher_query(
            sync_mode=sync_mode,
            cursor_field=cursor_field,
            stream_slice=stream_slice,
            stream_state=stream_state
        )

        return self._client.fetch_results(query, self._record_to_dict)




# Basic incremental stream
class IncrementalNeo4jEntityStream(Neo4jEntityStream, ABC):
    """
    Base class for incremental neo4j streams
    """

    def _set_config(self, config: Mapping[str, Any]) -> None:
        """
        Set stream config parameters
        """

        # set (approximative) maximum number of records per incremental sync
        max_records_per_incremental_sync = config.get("max_records_per_incremental_sync")
        if max_records_per_incremental_sync is not None:
            if not isinstance(max_records_per_incremental_sync, int):
                raise ValueError("max_records_per_incremental_sync must be an integer")

        self._max_records_per_incremental_sync = max_records_per_incremental_sync

        # set numbers of records between state checkpointing
        state_checkpoint_interval = config.get("state_checkpoint_interval") or 1000
        if state_checkpoint_interval is not None:
            if not isinstance(state_checkpoint_interval, int):
                raise ValueError("state_checkpoint_interval must be an integer")

        self._state_checkpoint_interval = state_checkpoint_interval

        # set number of slices per ncremental sync
        slices_count_per_incremental_sync = config.get("slices_count_per_incremental_sync")
        if slices_count_per_incremental_sync is not None:
            if not isinstance(slices_count_per_incremental_sync, int):
                raise ValueError("slices_count_per_incremental_sync must be an integer")

        self._slices_count_per_incremental_sync = slices_count_per_incremental_sync


    @property
    def state_checkpoint_interval(self) -> Optional[int]:
        """
        Decides how often to checkpoint state (i.e: emit a STATE message). E.g: if this returns a value of 100, then state is persisted after reading
        100 records, then 200, 300, etc.. A good default value is 1000 although your mileage may vary depending on the underlying data source.

        Checkpointing a stream avoids re-reading records in the case a sync is failed or cancelled.

        return None if state should not be checkpointed e.g: because records returned from the underlying data source are not returned in
        ascending order with respect to the cursor field. This can happen if the source does not support reading records in ascending order of
        created_at date (or whatever the cursor is). In those cases, state must only be saved once the full stream has been read.
        """
        return self._state_checkpoint_interval


    @property
    def source_defined_cursor(self) -> bool:
        """
        Return False if the cursor can be configured by the user.
        """
        return False


    @property
    def supports_incremental(self) -> bool:
        """
        Overrides Stream::supports_incremental to make it works with cursor field configured by user
        See https://github.com/airbytehq/airbyte/issues/7390
        :return: True if this stream supports incrementally reading data
        """
        return True


    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any], cursor_field: List[str]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        if isinstance(cursor_field, list):
            cursor_field = cursor_field[0]

        latest_state = latest_record[cursor_field]
        
        if current_stream_state and current_stream_state.get(cursor_field):
            return {cursor_field: max(latest_state, current_stream_state[cursor_field])}
        
        return {cursor_field: latest_state}


    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Generate the slices for this stream. See the stream slicing section of the docs for more information.

        :param stream_state:
        :return:
        """
        # disable slices if user did not configured slice count
        if  self._slices_count_per_incremental_sync is None:
            return [None]

        cursor_field = cursor_field[0]
        stream_state = stream_state or {}

        query_where = ""
        params = None
        query_cursor = self._get_cypher_identifier_for_cursor(cursor_field)
        if stream_state.get(cursor_field) is not None:
            query_where = "WHERE {} >= $from".format(query_cursor)
            params = {"from": stream_state.get(cursor_field)}

        data_info = self._describe_slices(query_cursor=query_cursor, query_where=query_where, params=params)

        records_count = data_info["records_count"]
        cursor_min = data_info["cursor_min"]
        cursor_max = data_info["cursor_max"]

        # if no data has to by synced, we return [None] i.e. no slice
        if records_count == 0:
            return [None]

        # Slice are supported for numbers only
        # because we do not know how to generate slices for other types, we return [None] i.e. no slice feature
        # get type of cursor_field
        cursor_types = self.get_json_schema()["properties"][cursor_field]["type"]

        if "number" not in cursor_types and "integer" not in cursor_types:
            return [None]

        # limit the numbers of records for this sync process if _max_records_per_incremental_sync has been set
        # and _max_records_per_incremental_sync < records_count
        if self._max_records_per_incremental_sync is not None and self._max_records_per_incremental_sync < records_count:
            percentile = round(self._max_records_per_incremental_sync / records_count, 3)

            cursor_values =  self._get_cursor_value_for_percentiles(
                percentiles=[percentile],
                query_cursor=query_cursor,
                cursor_min=cursor_min,
                cursor_max=cursor_max
            )

            cursor_max = cursor_values[percentile]
        

        # calculate percentiles to balance slices
        percentiles = [x/self._slices_count_per_incremental_sync for x in range(1, self._slices_count_per_incremental_sync, 1)]

        cursor_values =  self._get_cursor_value_for_percentiles(
            percentiles=percentiles,
            query_cursor=query_cursor,
            cursor_min=cursor_min,
            cursor_max=cursor_max
        )

        # if it is the initial sync (i.e. no cursor state), we decrease the cursor_min value by one, to be sure that
        # the cypher query include records with cursor_min  
        if stream_state.get(cursor_field) is None:
            cursor_min = cursor_min - 1

        slice_start_values = [cursor_values[percentile] for percentile in percentiles]
        slice_start_values.insert(0, cursor_min)

        slice_end_values = [cursor_values[percentile] for percentile in percentiles]
        slice_end_values.append(cursor_max)

        slices = []
        for item in zip(slice_start_values, slice_end_values):
            slices.append({"from": item[0], "to": item[1]})

        return slices


    def _get_cypher_query(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
        ) -> str:
        """
        :return: cypher query for fetching entity
        """
        cursor_field = cursor_field[0]
        
        latest_state = None
        if stream_state and stream_state.get("latest_state"):
            latest_state = stream_state.get("latest_state")
    
        query_where = ""
        query_order = ""
        query_limit = ""
        params = None

        # Add specific cursor_field WHERE and ORDER statements for incremental sync
        # See https://docs.airbyte.io/connector-development/cdk-python/incremental-stream
        query_cursor = self._get_cypher_identifier_for_cursor(cursor_field)
        if sync_mode == SyncMode.incremental:
            if stream_slice is not None:
                query_where = "WHERE {} > $from AND {} <= $to".format(query_cursor, query_cursor)
                params = {"from": stream_slice["from"], "to": stream_slice["to"]}
            elif latest_state is not None:
                query_where = "WHERE {} > $from".format(query_cursor)
                params = {"from": latest_state}

                # interval based checkpointing needs records to be sorted by cursor_field ASC
                query_order = "ORDER BY {} ASC".format(query_cursor)

                # limit result set per sync if user has enabled it
                if self._max_records_per_incremental_sync is not None:
                    query_limit = "LIMIT {}".format(str(self._max_records_per_incremental_sync))

 
        # Generate specfic MATCH statements corresponding to node or relationship
        if self.entity_type == "node":
            query_match = "({}:{})".format(self.name, self.name)
        elif self.entity_type == "relationship":
            query_match = "()-[{}:{}]->()".format(self.name, self.name)
        else:
            raise ValueError("Type {} not supported".format(self.entity_type))

        query = """
                MATCH {}
                {}
                RETURN {}
                {}
                {}
                """.format(
                        query_match,
                        query_where,
                        self.name,
                        query_order,
                        query_limit
                    )

        self.logger.debug(query)

        return {"query": query, "params": params}


    def _get_cypher_identifier_for_cursor(self, cursor_field: str) -> str:
        """
        :return: cursor identifier unsed in cypher queries
        """
        if cursor_field == "_identity":
            query_cursor = "ID({})".format(self.name)
        else:
            query_cursor = "{}.{}".format(self.name, cursor_field)

        return query_cursor


    def _describe_slices(self, query_cursor: str, query_where: str = "", params:Mapping[str, Any] = None) -> Mapping[str, Any]:
        """
        Get information about the slices and the cursor field
        :return: dict of information
        """
        info = {}
        
        # get statistical information
        # number of rows, min and max of cursor_field
        query = """
            MATCH ({}:{})
            {}
            RETURN
                count({}) AS count,
                min({}) AS min,
                max({}) AS max
        """.format(
                self.name, self.name,
                query_where,
                query_cursor,
                query_cursor,
                query_cursor
            )
        
        query = {"query": query, "params": params}
        res = next(self._client.fetch_results(query))

        info["records_count"] = res[0]
        info["cursor_min"] = res[1]
        info["cursor_max"] = res[2]

        # get type of cursor_field
        # query = """
        #     MATCH ({}:{})
        #     WHERE {} = $value
        #     RETURN
        #         apoc.meta.type({}) AS type
        # """.format(
        #         self.name, self.name,
        #         query_cursor,
        #         query_cursor
        #     )

        # params = {"value": info["cursor_min"]}
        # query = {"query": query, "params": params}
        # res = next(self._client.fetch_results(query))

        # info["cursor_type"] = res[0]


        return info


    def _get_cursor_value_for_percentiles(
        self,
        percentiles: List[int],
        query_cursor: str,
        cursor_min: Any,
        cursor_max: Any
    ) -> List[Mapping[int, Any]]:
        """
        Get cursor values matching percentiles
        :return: list of 
        """

        # get cursor field value corresponding to the percentiles
        query = """
            WITH $percentiles as percentiles
            CALL apoc.cypher.mapParallel2("
            MATCH ({}:{})
            WHERE {} >= {} AND {} <= {}
            RETURN percentileDisc({}, _) as cursor_value, _ as percentile",
            {{}}, percentiles, size(percentiles), 60) YIELD value
            RETURN value.percentile as percentile, value.cursor_value as cursor_value
            """.format(
                    self.name, self.name,
                    query_cursor, cursor_min, query_cursor, cursor_max,
                    query_cursor
                )
        params = {"percentiles": percentiles}
        query = {"query": query, "params": params}
        res = self._client.fetch_results(query)

        cursor_values = {record["percentile"]:record["cursor_value"] for record in res}
        
        return cursor_values



class NodeStream(IncrementalNeo4jEntityStream):
    """
    Stream corresponding to a node entity
    """
    primary_key = "identity"

    def __init__(self, label:str, client: Neo4jClient, config: Mapping[str, Any] = None) -> None:
        """
        :return: Set node label to stream
        """
        self._label = label
        super().__init__(client=client, config=config)


    @property
    def entity_type(self) -> str:
        """
        :return: Neo4j entity type (node, relationship)
        """
        return "node"


    @property
    def name(self) -> str:
        """
        :return: node label as the stream name
        """
        return self._label


    @staticmethod
    def _record_to_dict(record) -> Mapping[str, Any]:
        """
        Transform a record fetched by the python driver to dict
        :return: dict of the record object
        """
        if not isinstance(record[0], graph.Node):
            raise ValueError("Unknown record type: {}".format(type(record)))

        return_var_name = record.keys()[0]

        dict_record = record.data()[return_var_name]
        
        # add identity and labels not considered as properties
        # we prepend the names with an underscore to avoid collision with the name of a property
        if "_identity" in record.keys():
            raise ValueError("Unable to add '_identity' field to properties: a property with the same name exists")

        if "_labels" in record.keys():
            raise ValueError("Unable to add '_labels' field to properties: a property with the same name exists")

        dict_record["_identity"] = record[return_var_name].id
        dict_record["_labels"] = list(record[return_var_name].labels)

        return dict_record


class RelationshipStream(IncrementalNeo4jEntityStream):
    """
    Stream corresponding to a relationship entity
    """
    primary_key = "identity"


    def __init__(self, type:str, client: Neo4jClient, config: Mapping[str, Any] = None) -> None:
        """
        :return: Set node label to stream
        """
        self._type = type
        super().__init__(client=client, config=config)

    @property
    def entity_type(self) -> str:
        """
        :return: Neo4j entity type (node, relationship)
        """
        return "relationship"

    @property
    def name(self) -> str:
        """
        :return: Stream name. By default this is the implementing class name, but it can be overridden as needed.
        """
        return self._type

    @staticmethod
    def _record_to_dict(record) -> Mapping[str, Any]:
        """
        Transform a record fetched by the python driver to dict
        :return: dict of the record object
        """
        if not isinstance(record, data.Record):
            raise ValueError("Unknown record type: {}".format(type(record)))
            
        return_var_name = record.keys()[0]

        dict_record =  dict(record[return_var_name].items())
        
        # add identity, start, end and type not considered as properties
        # we prepend the names with an underscore to avoid collision with the name of a property
        if "_identity" in record.keys():
            raise ValueError("Unable to add '_identity' field to properties: a property with the same name exists")

        if "_start" in record.keys():
            raise ValueError("Unable to add '_start' field to properties: a property with the same name exists")

        if "_end" in record.keys():
            raise ValueError("Unable to add '_end' field to properties: a property with the same name exists")

        if "_type" in record.keys():
            raise ValueError("Unable to add '_type' field to properties: a property with the same name exists")

        dict_record["_identity"] = record[return_var_name].id
        dict_record["_start"] = record[return_var_name].start_node.id
        dict_record["_end"] = record[return_var_name].end_node.id
        dict_record["_type"] = record[return_var_name].type

        return record

