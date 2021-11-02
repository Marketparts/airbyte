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

from typing import Any, List, Mapping

from neo4j import GraphDatabase


class Neo4jClient:
    """
    Wrapper to the neo4j python driver
    """
    MAP_NEO4J_TYPES_TO_JSON_SCHEMA_TYPES = {
        "Boolean": "boolean",
        "Double": "number",
        "Long": "number",
        "String": "string",
        "StringArray": "array",
        "DoubleArray": "array",
        "LongArray": "array"
    }

    def __init__(self, config: Mapping[str, Any], preload_schemas: bool = False) -> None:
        self._config = config

        if preload_schemas:
            self.node_json_schemas
            self.relationship_json_schemas

    @property
    def uri(self) -> str:
        """
        Get connection uri for the driver
        :return: str
        """
        if not hasattr(self, "_uri"):
            self._uri = "{}://{}:{}".format(self._config['scheme'], self._config['host'], self._config['port'])

        return self._uri


    @property
    def driver(self) -> Any:
        """
        Get neo4j python driver
        :return: Any
        """
        if not hasattr(self, "_driver"):
            self._driver = GraphDatabase.driver(
                self.uri,
                auth=(self._config['username'], self._config['password'])
            )

        return self._driver


    @property
    def property_keys(self) -> List[str]:
        """
        Get keys of all existing properties in the database
        :return: list of property names
        """
        if not hasattr(self, "_property_keys"):
            query = "CALL db.propertyKeys();"
            transform_func = lambda x: x.get("propertyKey")
            
            results = self.fetch_results(cypher_query=query, transform_func=transform_func)
            self._property_keys = sorted([x for x in results], key=str.casefold)

        return self._property_keys


    @property
    def node_labels(self) -> List[str]:
        """
        Get labels of all existing nodes in the database
        :return: list of node labels
        """
        if not hasattr(self, "_node_labels"):
            query = "CALL db.labels();"
            transform_func = lambda x: x.get("label")

            results = self.fetch_results(cypher_query=query, transform_func=transform_func)
            self._node_labels = sorted([x for x in results], key=str.casefold)

        return self._node_labels


    @property
    def relationship_types(self) -> List[str]:
        """
        Get types of all existing relationships in the database
        :return: list of relationship types
        """
        if not hasattr(self, "_relationship_types"):
            query = "CALL db.relationshipTypes();"
            transform_func = lambda x: x.get("relationshipType")

            results = self.fetch_results(cypher_query=query, transform_func=transform_func)
            self._relationship_types = sorted([x for x in results], key=str.casefold)
        
        return self._relationship_types


    @property
    def node_json_schemas(self) -> Mapping[str, Any]:
        """
        Get schema of each existing nodes (i.e. properties and types)
        :return: dict schema
        """
        return self._get_node_json_schemas_v2()


    @property
    def relationship_json_schemas(self) -> Mapping[str, Any]:
        """
        Get schema of each existing relationships (i.e. properties and types)
        :return: dict schema
        """
        return self._get_relationship_json_schemas_v2()


    def _get_node_json_schemas_v2(self) -> Mapping[str, Any]:
        """
        Get schema of each existing nodes (i.e. properties and types)
        :return: dict schema
        """
        if not hasattr(self, "_node_json_schemas"):
            self._node_json_schemas = {}

            if len(self.node_labels) > 0:
                query = "CALL db.schema.nodeTypeProperties();"

                results = self.fetch_results(cypher_query=query)

                # we construct the json_schema from the resultset
                
                for label in self.node_labels:
                    self._node_json_schemas[label] = {}

                for record in results:
                    if record["propertyName"] is not None:
                        for label in record["nodeLabels"]:
                            self._node_json_schemas[label][record["propertyName"]] = {
                                "type": self._map_types_from_neo4j_to_json_schema(record["propertyTypes"]),
                                "required": record["mandatory"]
                                }

        return self._node_json_schemas


    def _get_relationship_json_schemas_v2(self) -> Mapping[str, Any]:
        """
        Get schema of each existing relationships (i.e. properties and types)
        :return: dict schema
        """
        if not hasattr(self, "_relationship_json_schemas"):
            self._relationship_json_schemas = {}

            if len(self.relationship_types) > 0:
                query = "CALL db.schema.relTypeProperties();"

                results = self.fetch_results(cypher_query=query)

                # we construct the json_schema from the resultset
                for type in self.relationship_types:
                    self._relationship_json_schemas[type] = {}

                for record in results:
                    if record["propertyName"] is not None:
                        type = record["relType"].replace("`", "").replace(":", "")
                        self._relationship_json_schemas[type][record["propertyName"]] = {
                            "type": self._map_types_from_neo4j_to_json_schema(record["propertyTypes"]),
                            "required": record["mandatory"]
                            }

        return self._relationship_json_schemas


    def _get_node_json_schemas_v1(self) -> Mapping[str, Any]:
        """
        Get schema of each existing nodes (i.e. properties and types)
        :return: dict schema
        """
        if not hasattr(self, "_node_json_schemas"):
            # we limit the variable returned by the neo4j procedure apoc.meta.nodeTypeProperties()
            return_variables = ["nodeType", "propertyName", "propertyTypes", "mandatory"]
            
            query_return_variables = ", ".join(return_variables)
            query = """CALL apoc.meta.nodeTypeProperties({{includeLabels:[_], sample: 1000}})
            YIELD {}
            RETURN _ as label, {}
            """.format(
                query_return_variables,
                query_return_variables
                )

            return_variables.insert(0, "label")

            # To accelerate the process, we parallelize the query as it will scan a sample of each node
            results = self.fetch_results_parallel(
                cypher_query=query,
                list_to_parallelize=self.node_labels,
                return_variables=return_variables
            )

            # we construct the json_schema from the resultset
            self._node_json_schemas = {}
            for label in self.node_labels:
                self._node_json_schemas[label] = {}

            for record in results:
                if record["propertyName"] is not None:
                    self._node_json_schemas[record["label"]][record["propertyName"]] = {
                        "type": self._map_types_from_neo4j_to_json_schema(record["propertyTypes"]),
                        "required": record["mandatory"]
                        }

        return self._node_json_schemas


    def _get_relationship_json_schemas_v1(self) -> Mapping[str, Any]:
        """
        Get schema of each existing relationships (i.e. properties and types)
        :return: dict schema
        """
        if not hasattr(self, "_relationship_json_schemas"):
            # we limit the variable returned by the neo4j procedure apoc.meta.relTypeProperties()
            return_variables = ["relType", "propertyName", "propertyTypes", "mandatory"]
            
            # To accelerate the process, we parallelize the query as it will scan a sample of each node
            query_return_variables = ", ".join(return_variables)
            query = """CALL apoc.meta.relTypeProperties({{includeRels:[_], maxRels: 100}})
            YIELD {}
            RETURN _ as type, {}
            """.format(
                query_return_variables,
                query_return_variables
                )

            return_variables.insert(0, "type")

            results = self.fetch_results_parallel(
                cypher_query=query,
                list_to_parallelize=self.relationship_types,
                return_variables=return_variables
                )

            # we construct the json_schema from the resultset
            self._relationship_json_schemas = {}
            for type in self.relationship_types:
                self._relationship_json_schemas[type] = {}

            for record in results:
                if record["propertyName"] is not None:
                    self._relationship_json_schemas[record["type"]][record["propertyName"]] = {
                        "type": self._map_types_from_neo4j_to_json_schema(record["propertyTypes"]),
                        "required": record["mandatory"]
                        }

        return self._relationship_json_schemas


    def fetch_results(self, cypher_query: Mapping[str, Any], transform_func = None) -> Any:
        """
        Fetch all results corresponding to the cypher query
        :return: list of records
        """
        if isinstance(cypher_query, str):
            cypher_query = {"query": cypher_query}
        elif cypher_query.get("query") is None:
            raise ValueError("Key 'query' not found in cypher query")

        # remove all trailing characters
        cypher_query["query"] = cypher_query["query"].rstrip()

        if transform_func is not None:
            if not callable(transform_func):
                raise TypeError("transform_func is not callable")
        else:
            transform_func = lambda x: x

        try:
            with self.driver.session() as session:
                if cypher_query.get("params") is not None:
                    results = session.read_transaction(self._do_cypher_tx, cypher_query.get("query"), **cypher_query.get("params"))
                else:
                    results = session.read_transaction(self._do_cypher_tx, cypher_query.get("query"))
                
                for ix, record in enumerate(results):
                    yield transform_func(record)

            self.driver.close()

        except Exception as e:
            # ensure connection is closed in case of exception
            self.driver.close()

            raise e
            

    def fetch_results_parallel(self, cypher_query: Mapping[str, Any], list_to_parallelize: List[Any], return_variables: List[str], partitions: int = None, transform_func = None) -> Any:
        """
        Fetch in parallel all results corresponding to the cypher query
        :return: list of records
        """
        partitions = partitions or len(list_to_parallelize)
        return_variables = ", ".join(["value.{} as {}".format(var, var) for var in return_variables])
        query = """
            WITH $list as list
            CALL apoc.cypher.mapParallel2("{}",
            {{}}, list, {}, 120) YIELD value
            RETURN {}
            """.format(
                    cypher_query,
                    partitions,
                    return_variables
                )
        params = {"list": list_to_parallelize}
        query = {"query": query, "params": params}

        return self.fetch_results(cypher_query=query, transform_func=transform_func)


    def verify_connectivity(self) -> Any:
        """
        Check if the stream is accessible
        Raises an exception if connection failed
        :return:
        """
        return self.driver.verify_connectivity()


    @staticmethod
    def _do_cypher_tx(tx, query: str, **kparams):
        """
        Execute cypher query
        :return: list of records
        """
        return list(tx.run(query, **kparams))


    def _map_types_from_neo4j_to_json_schema(self, types: List[str]) -> List[str]:
        """
        Get json_schema types corresponding to neo4j's ones 
        :return: list of types
        """
        if isinstance(types, str):
            types = [types]

        # Convert neo4j's types to json-schema's ones
        # See https://neo4j.com/labs/apoc/4.3/overview/apoc.meta/apoc.meta.type/
        # See http://json-schema.org/understanding-json-schema/reference/type.html
        
        json_schema_types = []

        for type in types:
            if type not in self.MAP_NEO4J_TYPES_TO_JSON_SCHEMA_TYPES:
                raise ValueError("Unable to map type to json_schema type : unknown type '{}'".format(type))
            
            json_schema_types.append(self.MAP_NEO4J_TYPES_TO_JSON_SCHEMA_TYPES[type])

        return json_schema_types