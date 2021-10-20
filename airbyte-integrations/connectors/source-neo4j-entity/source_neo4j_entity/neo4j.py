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

    def __init__(self, config: Mapping[str, Any]) -> None:
        self._config = config

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


    def fetch_results(self, cypher_query: Mapping[str, Any], transform_func = None) -> Any:
        """
        Fetch all results corresponding to the cypher query
        :return: list of records
        """
        if isinstance(cypher_query, str):
            cypher_query = {"query": cypher_query}
        elif cypher_query.get("query") is None:
            raise ValueError("Key 'query' not found in cypher query")

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

        except Exception as e:
            # ensure connection is closed in case of exception
            self.driver.close()

            raise e
            

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
