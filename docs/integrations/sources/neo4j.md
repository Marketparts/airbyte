# Neo4j

## Sync overview

This source synchronizes data from Neo4j's graph database as follows:
- nodes.
- relationships.
- simple custom cypher queries.

### Output schema

The output schema depends on the user configuration.

As Neo4j does not constrain schemas (e.g. the same node label can have different properties), properties and types can be discovered by two ways:
- Automatic discovery: for nodes and relationships only. It will scan a sample of the database to produce the corresponding schema. Internal Neo4j properties returned in Neo4j Browser are also added to the schemas: '_identity' and '_labels' for nodes, '_identity', '_start', '_end', '_type' for relationships.
- Json schemas specified by the user: for all type of streams. If provided, the connector will then use the json schemas provided, even if automatic discovery is enabled.


### Data type mapping

This section should contain a table mapping each of the connector's data types to Airbyte types. At the moment, Airbyte uses the same types used by [JSONSchema](https://json-schema.org/understanding-json-schema/reference/index.html). `string`, `date-time`, `object`, `array`, `boolean`, `integer`, and `number` are the most commonly used data types.

| Neo4j Type | Airbyte Type | Notes |
| :--- | :--- | :--- |
| `Boolean` | `boolean` |  |
| `Double` | `number` |  |
| `Long` | `number` |  |
| `String` | `string` |  |
| `StringArray` | `array` |  |
| `DoubleArray` | `array` |  |
| `LongArray` | `array` |  |


### Features

This section should contain a table with the following format:

| Feature | Supported? | Notes |
| :--- | :--- | :--- |
| Full Refresh Sync | Yes |  |
| Incremental Sync | Yes |  |
| Replicate Incremental Deletes | No |  |
| For databases, WAL/Logical replication | No |  |
| SSL connection | No |  |
| SSH Tunnel Support | No |  |

### Performance considerations

To lower risks on syncing high volumetry of data, this connector can limit the number of records transfered per incremental syncs (e.g. if a node has 20 millions of records, you can run 20 successive syncs of 1 million each to transfer all records).

As the distribution of cursor values is rarely linear, the source estimates the cursor value corresponding to the rounded percentage of records to be sync compared to the total number of records.
This cursor value is therefore approximate.

## Getting started

### Requirements

* Neo4j version: this connector has been successfully tested with Neo4j enterprise 3.5.14, 4.0.0, 4.2.0, 4.3.0. Tests fails for 4.1.0 so far.
* Neo4j plugins required: APOC Library.
* Network accessibility requirements: Neo4j must be accessible on unencrypted bolt/neo4j protocol. Http protocol and secured protocols (https, bolt+s/neo4j+s(+ssc)) have not been tested and therefore are not supported.
* Credentials/authentication requirements: a Neo4j user with at least read permissions on the targeted database. 

### Setup guide

#### 1. (optional) Create a dedicated read-only user

This step is optional but highly recommended to allow for better permission control and auditing. Alternatively, you can use Airbyte with an existing user in your database.

To create a dedicated database user, run the following commands against your database:


```sql
// For Neo4j 3.5
CALL dbms.security.createUser('airbyte', 'your_password_here', False);

// For Neo4j 4.0 and above
:use system;
CREATE USER 'airbyte' SET PASSWORD 'your_password_here';
```


Then grant the reader role to the user:
```sql
// For Neo4j 3.5
call dbms.security.addRoleToUser('reader', 'airbyte');

// For Neo4j 4.0 and above
:use system;
GRANT ROLE 'reader' TO 'airbyte';
```

N.B.: Neo4j 4.0 and above supports multiple databases and the reader role has access to all databases but system. To fine-grain access, you have to create a new role with specific privileges.
See Neo4j docs (https://neo4j.com/docs/cypher-manual/current/access-control/manage-privileges/)

#### 2. Make sure your database is accessible from the machine running Airbyte

This is dependent on your networking setup. The easiest way to verify if Airbyte is able to connect to your Neo4j instance is via the check connection tool in the UI.

#### 3. Add a new Neo4j source in the UI



#### 4. Configure the source in the UI
