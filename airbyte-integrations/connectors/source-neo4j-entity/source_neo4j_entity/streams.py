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
import inspect
from abc import ABC, abstractmethod, abstractproperty
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader

from neo4j import GraphDatabase, graph, data
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
        """
        Constructor
        """
        self._client = client
        config = config or {}

        self._set_config(config)

    @property
    def logger(self) -> AirbyteLogger:
        """
        Get logger
        Overrides Stream property as it returns a standard python logger instead of an AirbyteLogger
        :return: AirbyteLogger
        """
        return AirbyteLogger()


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
        base_schema["properties"] = {**base_schema["properties"], **self._get_specific_json_schema()}
        
        return base_schema


    @abstractmethod
    def _get_specific_json_schema(self) -> Mapping[str, Any]:
        """
        Add stream specific schema to the base schema
        """


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


    @abstractmethod
    def _get_cypher_match_query(self) -> str:
        """
        Get cypher MATCH query part
        :return: string cypher query
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


class IncrementalNeo4jEntityStream(Neo4jEntityStream, ABC):
    """
    Base class for incremental neo4j streams
    """

    def _set_config(self, config: Mapping[str, Any]) -> None:
        """
        Set stream config parameters
        """
        # Enables dynamic fetching stream schema from database
        # this will be used to return node primary keys when fetching relationships
        self._enable_dynamic_schemas = config.get("enable_dynamic_schemas", False)

        # Get json schemas configured by the user
        self._json_schemas = json.loads(config.get("json_schemas", "{}"))

        # Get incremental sync settings
        incremental_sync_settings = json.loads(config.get("incremental_sync_settings", "{}"))

        stream_incremental_sync_settings = incremental_sync_settings.get(self.name)
        if not isinstance(stream_incremental_sync_settings, Mapping):
            stream_incremental_sync_settings = incremental_sync_settings.get("DEFAULT")

        checkpointing_mode = None # disable incremental sync option by default
        max_records_per_incremental_sync = None # by default, there is no record limit
        state_checkpoint_interval = None # by default, no interval checkpointing is done
        slices_count_per_incremental_sync = None # by default, no slice is used

        if isinstance(stream_incremental_sync_settings, Mapping):
            # set checkpointing settings
            checkpointing_mode = stream_incremental_sync_settings.get("checkpointing_mode")
            if checkpointing_mode is None:
                raise ValueError(f"checkpointing_mode must be specified for stream {self.name}") 
            elif checkpointing_mode not in ["interval", "slices"]:
                raise ValueError(f"checkpointing_mode must be either 'interval' or 'slices' for stream {self.name}") 
            else:
                # set (approximative) maximum number of records per incremental sync
                max_records_per_incremental_sync = stream_incremental_sync_settings.get("max_records_per_incremental_sync")
                if max_records_per_incremental_sync and not isinstance(max_records_per_incremental_sync, int):
                    raise ValueError(f"max_records_per_incremental_sync for stream {self.name} must be an integer")

                if checkpointing_mode == "interval":
                    # set numbers of records between state checkpointing
                    state_checkpoint_interval = stream_incremental_sync_settings.get("state_checkpoint_interval")
                    if state_checkpoint_interval is None:
                        raise ValueError(f"state_checkpoint_interval for stream {self.name} must be specified")
                    elif not isinstance(state_checkpoint_interval, int):
                        raise ValueError(f"state_checkpoint_interval for stream {self.name} must be an integer")
                
                elif checkpointing_mode == "slices":
                    # set number of slices per incremental sync
                    slices_count_per_incremental_sync = stream_incremental_sync_settings.get("slices_count_per_incremental_sync")
                    if slices_count_per_incremental_sync is None:
                        raise ValueError(f"slices_count_per_incremental_sync for stream {self.name} must be specified")
                    elif not isinstance(slices_count_per_incremental_sync, int):
                        raise ValueError(f"slices_count_per_incremental_sync for stream {self.name} must be an integer")

        self._checkpointing_mode = checkpointing_mode
        self._max_records_per_incremental_sync = max_records_per_incremental_sync
        self._state_checkpoint_interval = state_checkpoint_interval
        self._slices_count_per_incremental_sync = slices_count_per_incremental_sync



        # # set (approximative) maximum number of records per incremental sync
        # max_records_per_incremental_sync = None

        # max_records_per_incremental_sync_config = config.get("max_records_per_incremental_sync")
        # if max_records_per_incremental_sync_config is not None:
        #     if isinstance(max_records_per_incremental_sync_config, int):
        #         max_records_per_incremental_sync = max_records_per_incremental_sync_config
        #     else:
        #         max_records_per_incremental_sync_config = json.loads(max_records_per_incremental_sync_config)
        #         max_records_per_incremental_sync = max_records_per_incremental_sync_config.get(self.name)

        #         if max_records_per_incremental_sync_config is not None and not isinstance(max_records_per_incremental_sync, int):
        #             raise ValueError("max_records_per_incremental_sync must be an integer")

        # self._max_records_per_incremental_sync = max_records_per_incremental_sync

        # # set numbers of records between state checkpointing
        # # by default, no interval checkpointing is done
        # state_checkpoint_interval = None
        # if config.get("incremental_sync_mode") is not None:
        #     state_checkpoint_interval = config["incremental_sync_mode"].get("state_checkpoint_interval") or 1000
        #     if state_checkpoint_interval is not None:
        #         if not isinstance(state_checkpoint_interval, int):
        #             raise ValueError("state_checkpoint_interval must be an integer")

        # self._state_checkpoint_interval = state_checkpoint_interval

        # # set number of slices per incremental sync
        # slices_count_per_incremental_sync = None
        # if config.get("incremental_sync_mode") is not None:
        #     slices_count_per_incremental_sync = config["incremental_sync_mode"].get("slices_count_per_incremental_sync")
        #     if slices_count_per_incremental_sync is not None:
        #         if not isinstance(slices_count_per_incremental_sync, int):
        #             raise ValueError("slices_count_per_incremental_sync must be an integer")

        # self._slices_count_per_incremental_sync = slices_count_per_incremental_sync


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
        return self._checkpointing_mode is not None


    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any], cursor_field: List[str]) -> Mapping[str, Any]:
        """
        Get the cursor field value of the latest record synchronized
        :return: dict {cursor_field: value}
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

        :param sync_mode:
        :param cursor_field:
        :param stream_state:
        :return: list of dict {"from": value, "to": value}
        """
        # if user has configured a full refresh
        # we return one empty slice
        if  sync_mode == SyncMode.full_refresh:
            return [None]

        # we proceed further for incremental sync
        if isinstance(cursor_field, str):
            cursor_field = [cursor_field]
    
        if not isinstance(cursor_field, List):
            raise ValueError("{} - {} - {} - {}".format(self.name, sync_mode, str(cursor_field), str(stream_state)))

        cursor_field = cursor_field[0]
        stream_state = stream_state or {}

        # Incremental sync slices are supported for numbers only
        # because we do not know how to generate slices for other types, we return [None] i.e. no slice
        cursor_types = self.get_json_schema()["properties"][cursor_field]["type"]

        if "number" not in cursor_types and "integer" not in cursor_types:
            raise ValueError("Type '{}' not supported for cursor_field '{}'".format(type(cursor_field), cursor_field))


        # get stats on data to see if there are any records to sync, and to prepare slices generation
        data_info = self._describe_slices(cursor_field=cursor_field, stream_state=stream_state)

        records_count = data_info["records_count"]
        cursor_min = data_info["cursor_min"]
        cursor_max = data_info["cursor_max"]

        # if no data has to by synced, we return no slice 
        if records_count == 0:
            slices = [{"from": stream_state.get(cursor_field), "to": stream_state.get(cursor_field)}]
        else:
            slices = self._get_cursor_slices(cursor_field=cursor_field, records_count=records_count, cursor_min=cursor_min, cursor_max=cursor_max)

            # if it is the initial sync (i.e. no cursor state), we decrease the cursor_min value by one, to be sure that
            # the cypher query include records with cursor_min  
            if stream_state.get(cursor_field) is None:
                slices[0]["from"] = slices[0]["from"] - 1

        return slices


    def _get_cursor_slices(self, cursor_field: List[str], records_count: int, cursor_min, cursor_max) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Generate cursor slices for incremental sync.
        
        This method will be used in:
        - "slices" mode i.e. if _slices_count_per_incremental_sync is configured by user
        - "standard" mode i.e. if _slices_count_per_incremental_sync is not configured by user, but _max_records_per_incremental_sync is

        :param stream_state:
        :return:
        """
        # limit the numbers of records for this sync process if _max_records_per_incremental_sync has been set
        # and _max_records_per_incremental_sync < records_count
        if self._max_records_per_incremental_sync is not None and self._max_records_per_incremental_sync < records_count:
            percentile = round(self._max_records_per_incremental_sync / records_count, 3)

            cursor_values =  self._get_cursor_value_for_percentiles(
                percentiles=[percentile],
                cursor_field=cursor_field,
                cursor_min=cursor_min,
                cursor_max=cursor_max
            )

            cursor_max = cursor_values[percentile]
        

        slices_count = self._slices_count_per_incremental_sync or 1

        if slices_count == 1:
            slice_start_values = []
            slice_end_values = []
        else:
            # calculate percentiles to balance slices
            percentiles = [x/self._slices_count_per_incremental_sync for x in range(1, self._slices_count_per_incremental_sync, 1)]

            cursor_values =  self._get_cursor_value_for_percentiles(
                percentiles=percentiles,
                cursor_field=cursor_field,
                cursor_min=cursor_min,
                cursor_max=cursor_max
            )

            slice_start_values = [cursor_values[percentile] for percentile in percentiles]
            slice_end_values = [cursor_values[percentile] for percentile in percentiles]


        slice_start_values.insert(0, cursor_min)
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
        query_where = ""
        query_order = ""
        params = None

        if sync_mode == SyncMode.incremental:
            # Add specific cursor_field WHERE and ORDER statements for incremental sync
            # See https://docs.airbyte.io/connector-development/cdk-python/incremental-stream
            cursor_field = cursor_field[0]
            query_cursor = self._get_cypher_identifier_for_cursor(cursor_field)

            if self._checkpointing_mode == "interval":
                # # if sync mode is incremental but no slices have been configured
                # # we get cursor_values from and to for 1 slice
                # stream_slice = self._get_cursor_slices(sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state)[0]

                # interval based checkpointing needs records to be sorted by cursor_field ASC
                query_order = "ORDER BY {} ASC".format(query_cursor)


            query_where = "WHERE {} > $from AND {} <= $to".format(query_cursor, query_cursor)
            params = {"from": stream_slice["from"], "to": stream_slice["to"]}

 
        # Generate specific MATCH statements corresponding to node or relationship
        query_match = self._get_cypher_match_query()

        # Generate specific RETURN statements corresponding to node or relationship
        query_return = self._get_cypher_return_query()

        query = "{} {} {} {}".format(
            query_match,
            query_where,
            query_return,
            query_order
        )

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


    def _describe_slices(self, cursor_field: List[str], stream_state: Mapping[str, Any] = None) -> Mapping[str, Any]:
        """
        Get information about the slices and the cursor field
        :return: dict of information
        """
        stream_state = stream_state or {}
        if isinstance(cursor_field, str):
            cursor_field = [cursor_field]
        
        cursor_field = cursor_field[0]

        
        # get statistical information
        # number of rows, min and max of cursor_field
        info = {}

        query_where = ""
        params = None
        query_cursor = self._get_cypher_identifier_for_cursor(cursor_field)
        
        if stream_state.get(cursor_field) is not None:
            query_where = "WHERE {} >= $from".format(query_cursor)
            params = {"from": stream_state.get(cursor_field)}

        query = """
            {}
            {}
            RETURN
                count({}) AS count,
                min({}) AS min,
                max({}) AS max
        """.format(
                self._get_cypher_match_query(),
                query_where,
                query_cursor,
                query_cursor,
                query_cursor
            )
        
        query = {"query": query, "params": params}
        res = next(self._client.fetch_results(query))

        # if records_count equals 0 (i.e. stream_state higher than cursor max),
        # cursor_min and cursor_max will be None, so we set them to the stream_state value
        info["records_count"] = res[0]
        info["cursor_min"] = res[1]
        info["cursor_max"] = res[2]

        return info


    def _get_cursor_value_for_percentiles(
        self,
        percentiles: List[int],
        cursor_field: List[str],
        cursor_min: Any,
        cursor_max: Any
    ) -> List[Mapping[int, Any]]:
        """
        Get cursor values matching percentiles
        :return: list of 
        """
        if isinstance(cursor_field, str):
            cursor_field = [cursor_field]
        
        cursor_field = cursor_field[0]

        query_cursor = self._get_cypher_identifier_for_cursor(cursor_field)

        # get cursor field value corresponding to the percentiles
        query = """
            WITH $percentiles as percentiles
            CALL apoc.cypher.mapParallel2("
            {}
            WHERE {} >= {} AND {} <= {}
            RETURN percentileDisc({}, _) as cursor_value, _ as percentile",
            {{}}, percentiles, size(percentiles), 60) YIELD value
            RETURN value.percentile as percentile, value.cursor_value as cursor_value
            """.format(
                    self._get_cypher_match_query(),
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

    def __init__(self, label:str, client: Neo4jClient, config: Mapping[str, Any] = None) -> None:
        """
        Constructor
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
        :return: node label as stream name
        """
        return self._label


    @staticmethod
    def _record_to_dict(record) -> Mapping[str, Any]:
        """
        Transform a record fetched by the python driver to dict
        :return: dict of the record object
        """
        if not isinstance(record, data.Record):
            raise ValueError("Unknown record type: {}".format(type(record)))
        
        key = record.keys()[0]
        if not isinstance(record[key], graph.Node):
            raise ValueError("Unknown record type: {}".format(type(record[key])))


        # add identity and labels not considered as properties
        dict_record = {
            "_identity": record[key].id,
            "_labels": list(record[key].labels)
        }

        dict_record = {**dict_record, **dict(record[key].items())}

        return dict_record


    def _get_specific_json_schema(self) -> Mapping[str, Any]:
        """
        Add stream specific schema to the base schema
        """
        schema = self._json_schemas.get(self.name, {})
        
        if self._enable_dynamic_schemas:
            schema = self._client.node_json_schemas[self.name]

        return schema


    def _get_cypher_match_query(self) -> str:
        """
        Get cypher MATCH query part
        :return: string cypher query
        """
        return f"MATCH ({self.name}:{self.name})"


    def _get_cypher_return_query(self) -> str:
        """
        Get cypher RETURN query part
        :return: string cypher query
        """
        return f"RETURN {self.name}"



class RelationshipStream(IncrementalNeo4jEntityStream):
    """
    Stream corresponding to a relationship entity
    """

    def __init__(self, type:str, client: Neo4jClient, config: Mapping[str, Any] = None) -> None:
        """
        Constructor
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
        :return: relationship type as stream name
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
        
        key = record.keys()[0]
        if not isinstance(record[key], graph.Relationship):
            raise ValueError("Unknown record type: {}".format(type(record[key])))

        # add identity, start, end and type not considered as properties
        dict_record = {
            "_identity": record[key].id,
            "_start": record[key].start_node.id,
            "_end": record[key].end_node.id,
            "_type": record[key].type
        }

        dict_record = {**dict_record, **dict(record[key].items())}

        return dict_record


    def _get_specific_json_schema(self) -> Mapping[str, Any]:
        """
        Add stream specific schema to the base schema
        """
        schema = self._json_schemas.get(self.name, {})
        
        if self._enable_dynamic_schemas:
            schema = self._client.relationship_json_schemas[self.name]

        return schema


    def _get_cypher_match_query(self) -> str:
        """
        Get cypher MATCH query part
        :return: string cypher query
        """
        return f"MATCH ()-[{self.name}:{self.name}]->()"


    def _get_cypher_return_query(self) -> str:
        """
        Get cypher RETURN query part
        :return: string cypher query
        """
        return f"RETURN {self.name}"


class CypherStream(IncrementalNeo4jEntityStream):
    """
    Stream corresponding to a custom cypher query
    """

    def __init__(self, name:str, client: Neo4jClient, config: Mapping[str, Any] = None) -> None:
        """
        Constructor
        """
        self._name = name

        custom_streams = json.loads(config.get("custom_streams", "{}"))
        if  isinstance(custom_streams, Mapping):
            stream_config = custom_streams.get(self.name)
            if stream_config:
                self._match_query = stream_config.get("match_query")
                self._return_query = stream_config.get("return_query")
                self._cursor_field = stream_config.get("cursor_field")

            if not self._match_query:
                raise ValueError("match_query configuration parameter is missing")
            if not self._return_query:
                raise ValueError("return_query configuration parameter is missing")

        super().__init__(client=client, config=config)


    @property
    def entity_type(self) -> str:
        """
        :return: Neo4j entity type (node, relationship, cypher)
        """
        return "cypher"


    @property
    def name(self) -> str:
        """
        :return: custom name as stream name
        """
        return self._name


    @property
    def cursor_field(self) -> Union[str, List[str]]:
        """
        Get the cursor field defined by the user
        :return: The name of the field used as a cursor. If the cursor is nested, return an array consisting of the path to the cursor.
        """
        return self._cursor_field

    @property
    def source_defined_cursor(self) -> bool:
        """
        Return True as the cursor field must be defined in the source configuration
        """
        return True


    @staticmethod
    def _record_to_dict(record) -> Mapping[str, Any]:
        """
        Transform a record fetched by the python driver to dict
        :return: dict of the record object
        """
        if not isinstance(record, data.Record):
            raise ValueError("Unknown record type: {}".format(type(record)))
        
        dict_record = dict()

        for key in record.keys():
            if isinstance(record[key], graph.Relationship):
                # add identity, start, end and type not considered as properties
                # we prepend the names with an underscore to avoid collision with the name of a property
                dict_key = {
                    "_identity": record[key].id,
                    "_start": record[key].start_node.id,
                    "_end": record[key].end_node.id,
                    "_type": record[key].type
                }
                
                # add relationship properties
                dict_props = dict(record[key].items())
                dict_key = {**dict_key, **dict_props}

                # prefix each returned props with key
                dict_key = dict((f"{key}.{name}", val) for name, val in dict_key.items())

            elif isinstance(record[key], graph.Node):
                # add identity, and labels not considered as properties
                # we prepend the names with an underscore to avoid collision with the name of a property
                dict_key = {
                    "_identity": record[key].id,
                    "_labels": list(record[key].labels)
                }
                
                # add node properties
                dict_props = dict(record[key].items())
                dict_key = {**dict_key, **dict_props}

                # prefix each returned props with key
                dict_key = dict((f"{key}.{name}", val) for name, val in dict_key.items())

            else:
                dict_key = {key: record[key]}

            key_intersect = list(set(dict_record.keys()) & set(dict_key))
            if len(key_intersect) > 0:
                raise ValueError("Several properties have the same keys {}".format(str(key_intersect)))

            dict_record = {**dict_record, **dict_key}


        return dict_record


    def _get_specific_json_schema(self) -> Mapping[str, Any]:
        """
        Add stream specific schema to the base schema
        """
        # return cursor field type as incremental sync supports a few types only
        schema = self._json_schemas.get(self.name, {})
        
        return schema


    def _get_cypher_match_query(self) -> str:
        """
        Get cypher MATCH query part
        :return: string cypher query
        """
        return self._match_query


    def _get_cypher_return_query(self) -> str:
        """
        Get cypher RETURN query part
        :return: string cypher query
        """
        return self._return_query

    def _get_cypher_identifier_for_cursor(self, cursor_field: str) -> str:
        """
        :return: cursor identifier unsed in cypher queries
        """
        return cursor_field
