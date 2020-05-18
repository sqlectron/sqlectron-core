import sqlite3 from 'sqlite3';
import { identify } from 'sql-query-identifier';

import createLogger from '../../logger';

const logger = createLogger('db:clients:sqlite');

const sqliteErrors = {
  CANCELED: 'SQLITE_INTERRUPT',
};


export default async function (server, database) {
  const dbConfig = configDatabase(server, database);
  logger().debug('create driver client for sqlite3 with config %j', dbConfig);

  const conn = { dbConfig };

  // light solution to test connection with with the server
  const version = (await driverExecuteQuery(conn, { query: 'SELECT sqlite_version() as version' })).data[0].version;

  return {
    wrapIdentifier,
    version,
    disconnect: () => disconnect(conn),
    listTables: () => listTables(conn),
    listViews: () => listViews(conn),
    listRoutines: () => listRoutines(conn),
    listTableColumns: (db, table) => listTableColumns(conn, db, table),
    listTableTriggers: (table) => listTableTriggers(conn, table),
    listTableIndexes: (db, table) => listTableIndexes(conn, db, table),
    listSchemas: () => listSchemas(conn),
    getTableReferences: (table) => getTableReferences(conn, table),
    getTableKeys: (db, table) => getTableKeys(conn, db, table),
    query: (queryText) => query(conn, queryText),
    executeQuery: (queryText) => executeQuery(conn, queryText),
    listDatabases: () => listDatabases(conn),
    getQuerySelectTop: (table, limit) => getQuerySelectTop(conn, table, limit),
    getTableCreateScript: (table) => getTableCreateScript(conn, table),
    getViewCreateScript: (view) => getViewCreateScript(conn, view),
    getRoutineCreateScript: (routine) => getRoutineCreateScript(conn, routine),
    truncateAllTables: () => truncateAllTables(conn),
  };
}


export function disconnect() {
  // SQLite does not have connection poll. So we open and close connections
  // for every query request. This allows multiple request at same time by
  // using a different thread for each connection.
  // This may cause connection limit problem. So we may have to change this at some point.
  return Promise.resolve();
}


export function wrapIdentifier(value) {
  if (value === '*') return value;
  const matched = value.match(/(.*?)(\[[0-9]\])/); // eslint-disable-line no-useless-escape
  if (matched) return wrapIdentifier(matched[1]) + matched[2];
  return `"${value.replace(/"/g, '""')}"`;
}


export function getQuerySelectTop(client, table, limit) {
  return `SELECT * FROM ${wrapIdentifier(table)} LIMIT ${limit}`;
}

export function query(conn, queryText) {
  let queryConnection = null;

  return {
    execute() {
      return runWithConnection(conn, async (connection) => {
        try {
          queryConnection = connection;

          const result = await executeQuery({ connection }, queryText);

          return result;
        } catch (err) {
          if (err.code === sqliteErrors.CANCELED) {
            err.sqlectronError = 'CANCELED_BY_USER';
          }

          throw err;
        }
      });
    },

    async cancel() {
      if (!queryConnection) {
        throw new Error('Query not ready to be canceled');
      }

      queryConnection.interrupt();
    },
  };
}


export async function executeQuery(conn, queryText) {
  const result = await driverExecuteQuery(conn, { query: queryText, multiple: true });

  return result.map(parseRowQueryResult);
}


export async function listTables(conn) {
  const sql = `
    SELECT name
    FROM sqlite_master
    WHERE type='table'
    ORDER BY name
  `;

  const { data } = await driverExecuteQuery(conn, { query: sql });

  return data;
}

export async function listViews(conn) {
  const sql = `
    SELECT name
    FROM sqlite_master
    WHERE type = 'view'
  `;

  const { data } = await driverExecuteQuery(conn, { query: sql });

  return data;
}

export function listRoutines() {
  return Promise.resolve([]); // DOES NOT SUPPORT IT
}

export async function listTableColumns(conn, database, table) {
  const sql = `PRAGMA table_info('${table}')`;

  const { data } = await driverExecuteQuery(conn, { query: sql });

  return data.map((row) => ({
    columnName: row.name,
    dataType: row.type,
  }));
}

export async function listTableTriggers(conn, table) {
  const sql = `
    SELECT name
    FROM sqlite_master
    WHERE type = 'trigger'
      AND tbl_name = '${table}'
  `;

  const { data } = await driverExecuteQuery(conn, { query: sql });

  return data.map((row) => row.name);
}

export async function listTableIndexes(conn, database, table) {
  const sql = `PRAGMA INDEX_LIST('${table}')`;

  const { data } = await driverExecuteQuery(conn, { query: sql });

  return data.map((row) => row.name);
}

export function listSchemas() {
  return Promise.resolve([]); // DOES NOT SUPPORT IT
}

export async function listDatabases(conn) {
  const result = await driverExecuteQuery(conn, { query: 'PRAGMA database_list;' });

  return result.data.map((row) => row.file || ':memory:');
}

export function getTableReferences() {
  return Promise.resolve([]); // TODO: not implemented yet
}

export function getTableKeys() {
  return Promise.resolve([]); // TODO: not implemented yet
}

export async function getTableCreateScript(conn, table) {
  const sql = `
    SELECT sql
    FROM sqlite_master
    WHERE name = '${table}';
  `;

  const { data } = await driverExecuteQuery(conn, { query: sql });

  return data.map((row) => row.sql);
}

export async function getViewCreateScript(conn, view) {
  const sql = `
    SELECT sql
    FROM sqlite_master
    WHERE name = '${view}';
  `;

  const { data } = await driverExecuteQuery(conn, { query: sql });

  return data.map((row) => row.sql);
}

export function getRoutineCreateScript() {
  return Promise.resolve([]); // DOES NOT SUPPORT IT
}

export async function truncateAllTables(conn) {
  await runWithConnection(conn, async (connection) => {
    const connClient = { connection };

    const tables = await listTables(connClient);

    const truncateAll = tables.map((table) => `
      DELETE FROM ${table.name};
    `).join('');

    // TODO: Check if sqlite_sequence exists then execute:
    // DELETE FROM sqlite_sequence WHERE name='${table}';

    await driverExecuteQuery(connClient, { query: truncateAll });
  });
}


function configDatabase(server, database) {
  return {
    database: database.database,
  };
}


function parseRowQueryResult({ data, statement, changes }) {
  // Fallback in case the identifier could not reconize the command
  const isSelect = Array.isArray(data);
  const rows = data || [];
  return {
    command: statement.type || (isSelect && 'SELECT'),
    rows,
    fields: Object.keys(rows[0] || {}).map((name) => ({ name })),
    rowCount: data && data.length,
    affectedRows: changes || 0,
  };
}


function identifyCommands(queryText) {
  try {
    return identify(queryText, { strict: false });
  } catch (err) {
    return [];
  }
}

export function driverExecuteQuery(conn, queryArgs) {
  const runQuery = (connection, { executionType, text }) => new Promise((resolve, reject) => {
    const method = resolveExecutionType(executionType);
    connection[method](text, queryArgs.params, function driverExecQuery(err, data) {
      if (err) { return reject(err); }

      return resolve({
        data,
        lastID: this.lastID,
        changes: this.changes,
      });
    });
  });

  const identifyStatementsRunQuery = async (connection) => {
    const statements = identifyCommands(queryArgs.query);

    const results = await Promise.all(
      statements.map(async (statement) => {
        const result = await runQuery(connection, statement);

        return {
          ...result,
          statement,
        };
      }),
    );

    return queryArgs.multiple ? results : results[0];
  };

  return conn.connection
    ? identifyStatementsRunQuery(conn.connection)
    : runWithConnection(conn, identifyStatementsRunQuery);
}

function runWithConnection(conn, run) {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(conn.dbConfig.database, async (err) => {
      if (err) {
        reject(err);
        return;
      }

      try {
        db.serialize();
        const results = await run(db);
        resolve(results);
      } catch (runErr) {
        reject(runErr);
      } finally {
        db.close();
      }
    });
  });
}

function resolveExecutionType(executioType) {
  switch (executioType) {
    case 'MODIFICATION': return 'run';
    default: return 'all';
  }
}
