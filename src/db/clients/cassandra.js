import { Client } from 'cassandra-driver';
import { identify } from 'sql-query-identifier';

import createLogger from '../../logger';

const logger = createLogger('db:clients:cassandra');

/**
 * To keep compatibility with the other clients we treat keyspaces as database.
 */

export default function (server, database) {
  return new Promise(async (resolve, reject) => {
    const dbConfig = configDatabase(server, database);

    logger().debug('creating database client %j', dbConfig);
    const client = new Client(dbConfig);

    logger().debug('connecting');
    client.connect((err) => {
      if (err) {
        client.shutdown();
        return reject(err);
      }

      logger().debug('connected');
      resolve({
        wrapIdentifier,
        disconnect: () => disconnect(client),
        listTables: (db) => listTables(client, db),
        listViews: () => listViews(client),
        listRoutines: () => listRoutines(client),
        listTableColumns: (db, table) => listTableColumns(client, db, table),
        listTableTriggers: (table) => listTableTriggers(client, table),
        listTableIndexes: (db, table) => listTableIndexes(client, table),
        listSchemas: () => listSchemas(client),
        getTableReferences: (table) => getTableReferences(client, table),
        getTableKeys: (db, table) => getTableKeys(client, db, table),
        query: (queryText) => executeQuery(client, queryText),
        executeQuery: (queryText) => executeQuery(client, queryText),
        listDatabases: () => listDatabases(client),
        getQuerySelectTop: (table, limit) => getQuerySelectTop(client, table, limit),
        getTableCreateScript: (table) => getTableCreateScript(client, table),
        getViewCreateScript: (view) => getViewCreateScript(client, view),
        getRoutineCreateScript: (routine) => getRoutineCreateScript(client, routine),
        truncateAllTables: (db) => truncateAllTables(client, db),
      });
    });
  });
}


export function disconnect(client) {
  client.shutdown();
}


export function listTables(client, database) {
  return isLegacyVersion(client)
    .then((isLegacy) => {
      if (isLegacy) {
        // for Cassandra 2.x
        return `SELECT columnfamily_name as name
          FROM system.schema_columnfamilies
          WHERE keyspace_name = ?
        `;
      }
      return `SELECT table_name as name
          FROM system_schema.tables
          WHERE keyspace_name = ?
        `;
    })
    .then((cql) => {
      const params = [database];
      return new Promise((resolve, reject) => {
        client.execute(cql, params, (err, data) => {
          if (err) reject(err);
          resolve(data.rows.map((row) => ({ name: row.name })));
        });
      });
    });
}

export function listViews() {
  return Promise.resolve([]);
}

export function listRoutines() {
  return Promise.resolve([]);
}

export function listTableColumns(client, database, table) {
  return isLegacyVersion(client)
    .then((isLegacy) => {
      const cqlText = isLegacy
       ? `SELECT type as position, column_name, validator as type
            FROM system.schema_columns
            WHERE keyspace_name = ? AND columnfamily_name = ?`
       : `SELECT position, column_name, type
            FROM system_schema.columns
            WHERE keyspace_name = ? AND table_name = ?`;
      return { text: cqlText, isLegacy };
    }).then((cql) => {
      const params = [
        database,
        table,
      ];
      return new Promise((resolve, reject) =>
        client.execute(cql.text, params, (err, data) => {
          if (err) reject(err);
          if (cql.isLegacy) { // Cassandra 2.x
            resolve(data.rows
              .sort((a, b) => +(a.position > b.position) || -(a.position < b.position))
              .map((row) => ({
                columnName: row.column_name,
                dataType: mapLegacyDataTypes(row.type),
              }))
            );
          } else {
            resolve(data.rows
              // force pks be placed at the results beginning
              .sort((a, b) => b.position - a.position)
              .map((row) => ({
                columnName: row.column_name,
                dataType: row.type,
              }))
            );
          }
        })
      );
    });
}

/**
 * The system schema of Casandra 2.x does not have data type, but only validator
 * classes. To make the behavior consistent with v3.x, we try to deduce the
 * correponding CQL data type using the validator name.
 * @param {string} validator
 * @returns {string}
 */
function mapLegacyDataTypes(validator) {
  const type = validator.split('.').pop();
  switch (type) {
    case 'Int32Type':
    case 'LongType':
      return 'int';
    case 'UTF8Type':
      return 'text';
    case 'TimestampType':
    case 'DateType':
      return 'timestamp';
    case 'DoubleType':
      return 'double';
    case 'FloatType':
      return 'float';
    case 'UUIDType':
      return 'uuid';
    case 'CounterColumnType':
      return 'counter';
    default:
      logger().debug('validator %s is not yet mapped!', validator);
      return type;
  }
}

export function listTableTriggers() {
  return Promise.resolve([]);
}
export function listTableIndexes() {
  return Promise.resolve([]);
}

export function listSchemas() {
  return Promise.resolve([]);
}

export function getTableReferences() {
  return Promise.resolve([]);
}

export function getTableKeys(client, database, table) {
  return client.metadata
    .getTable(database, table)
    .then((tableInfo) => tableInfo
      .partitionKeys
      .map((key) => ({
        constraintName: null,
        columnName: key.name,
        referencedTable: null,
        keyType: 'PRIMARY KEY',
      }))
    );
}

function query(conn, queryText) { // eslint-disable-line no-unused-vars
  throw new Error('"query" function is not implementd by cassandra client.');
}

export function executeQuery(client, queryText) {
  const commands = identifyCommands(queryText).map((item) => item.type);

  return new Promise((resolve, reject) => {
    client.execute(queryText, (err, data) => {
      if (err) return reject(err);

      resolve([parseRowQueryResult(data, commands[0])]);
    });
  });
}


export function listDatabases(client) {
  return new Promise((resolve) => {
    resolve(Object.keys(client.metadata.keyspaces));
  });
}


export function getQuerySelectTop(client, table, limit) {
  return `SELECT * FROM ${wrapIdentifier(table)} LIMIT ${limit}`;
}

export function getTableCreateScript() {
  return Promise.resolve([]);
}

export function getViewCreateScript() {
  return Promise.resolve([]);
}

export function getRoutineCreateScript() {
  return Promise.resolve([]);
}

export function wrapIdentifier(value) {
  if (value === '*') return value;
  const matched = value.match(/(.*?)(\[[0-9]\])/); // eslint-disable-line no-useless-escape
  if (matched) return wrapIdentifier(matched[1]) + matched[2];
  return `"${value.replace(/"/g, '""')}"`;
}


export const truncateAllTables = async (connection, database) => {
  const result = await listTables(connection, database);
  const tables = result.map((table) => table.name);
  const promises = tables.map((t) => {
    const truncateSQL = `
      TRUNCATE TABLE ${wrapIdentifier(database)}.${wrapIdentifier(t)};
    `;
    return executeQuery(connection, truncateSQL);
  });

  await Promise.all(promises);
};

function configDatabase(server, database) {
  const config = {
    contactPoints: [server.config.host],
    protocolOptions: {
      port: server.config.port,
    },
    keyspace: database.database,
  };

  if (server.sshTunnel) {
    config.contactPoints = [server.config.localHost];
    config.protocolOptions.port = server.config.localPort;
  }

  if (server.config.ssl) {
    // TODO: sslOptions
  }

  return config;
}

/**
 * Connects to the server cluster and determine if it is a legacy (<3.0) version
 * @param {Client} client
 * @returns {Promise<boolean>}
 */
function isLegacyVersion(client) {
  return client.connect()
    .then(() => {
      let cassandraVersion = '3.0.0';
      try {
        cassandraVersion = client.getState().getConnectedHosts()[0].cassandraVersion;
      } catch (err) {
        logger().debug(err);
      }
      return cassandraVersion.split('.')[0] < '3';
    });
}

function parseRowQueryResult(data, command) {
  // Fallback in case the identifier could not reconize the command
  const isSelect = command ? command === 'SELECT' : Array.isArray(data.rows);
  return {
    command: command || (isSelect && 'SELECT'),
    rows: data.rows || [],
    fields: data.columns || [],
    rowCount: isSelect ? (data.rowLength || 0) : undefined,
    affectedRows: !isSelect && !isNaN(data.rowLength) ? data.rowLength : undefined,
  };
}


function identifyCommands(queryText) {
  try {
    return identify(queryText);
  } catch (err) {
    return [];
  }
}
