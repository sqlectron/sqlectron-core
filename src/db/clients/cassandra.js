import * as cassandra from 'cassandra-driver';
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
    const client = new cassandra.Client(dbConfig);

    logger().debug('connecting');
    client.connect(async (err) => {
      if (err) {
        client.shutdown();
        return reject(err);
      }

      client.version = client.getState().getConnectedHosts()[0].getCassandraVersion();

      logger().debug('connected');
      resolve({
        wrapIdentifier,
        version: client.getState().getConnectedHosts()[0].cassandraVersion,
        getVersion: () => client.getState().getConnectedHosts()[0].cassandraVersion,
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
  return new Promise((resolve, reject) => {
    let sql;
    if (client.version[0] === 2) {
      sql = `
        SELECT columnfamily_name as name
        FROM system.schema_columnfamilies
        WHERE keyspace_name = ?
      `;
    } else {
      sql = `
        SELECT table_name as name
        FROM system_schema.tables
        WHERE keyspace_name = ?
      `;
    }

    const params = [database];
    client.execute(sql, params, (err, data) => {
      if (err) return reject(err);
      resolve(data.rows.map((row) => ({ name: row.name })));
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
  const cassandra2 = client.version[0] === 2;
  return new Promise((resolve, reject) => {
    let sql;
    if (cassandra2) {
      sql = `
        SELECT type as position, column_name, validator as type
        FROM system.schema_columns
        WHERE keyspace_name = ?
          AND columnfamily_name = ?
      `;
    } else {
      sql = `
        SELECT position, column_name, type
        FROM system_schema.columns
        WHERE keyspace_name = ?
          AND table_name = ?
      `;
    }
    const params = [
      database,
      table,
    ];
    client.execute(sql, params, (err, data) => {
      if (err) return reject(err);
      resolve(
        data.rows
          // force pks be placed at the results beginning
          .sort((a, b) => {
            if (cassandra2) {
              return (+(a.position > b.position) || -(a.position < b.position));
            }
            return b.position - a.position;
          }).map((row) => {
            const rowType = cassandra2 ? mapLegacyDataTypes(row.type) : row.type;
            return {
              columnName: row.column_name,
              dataType: rowType,
            };
          }),
      );
    });
  });
}

/**
 * The system schema of Casandra 2.x does not have data type, but only validator
 * classes. To make the behavior consistent with v3.x, we try to deduce the
 * correponding CQL data type using the validator name.
 *
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
      })));
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

  // client authentication
  if (server.config.user && server.config.password) {
    const user = server.config.user;
    const password = server.config.password;
    const authProviderInfo = new cassandra.auth.PlainTextAuthProvider(user, password);
    config.authProvider = authProviderInfo;
  }

  return config;
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
