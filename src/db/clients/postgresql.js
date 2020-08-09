import pg from 'pg';
import { identify } from 'sql-query-identifier';

import { buildDatabseFilter, buildSchemaFilter } from './utils';
import createLogger from '../../logger';
import { createCancelablePromise, versionCompare } from '../../utils';
import errors from '../../errors';

const logger = createLogger('db:clients:postgresql');

const pgErrors = {
  CANCELED: '57014',
};

/**
 * Do not convert DATE types to JS date.
 * It gnores of applying a wrong timezone to the date.
 * TODO: do not convert as well these same types with array (types 1115, 1182, 1185)
 */
pg.types.setTypeParser(1082, 'text', (val) => val); // date
pg.types.setTypeParser(1114, 'text', (val) => val); // timestamp without timezone
pg.types.setTypeParser(1184, 'text', (val) => val); // timestamp


export default async function (server, database) {
  const dbConfig = configDatabase(server, database);
  logger().debug('create driver client for postgres with config %j', dbConfig);

  const conn = {
    pool: new pg.Pool(dbConfig),
  };

  logger().debug('connected');
  const defaultSchema = await getSchema(conn);

  const version = (await driverExecuteQuery(conn, { query: 'select version()' })).rows[0].version;
  const splitVersion = version.split(' ');

  conn.versionData = {
    name: splitVersion[0],
    version: splitVersion[1],
    string: version,
  };

  return {
    /* eslint max-len:0 */
    wrapIdentifier,
    version,
    getVersion: () => conn.versionData,
    disconnect: () => disconnect(conn),
    listTables: (db, filter) => listTables(conn, filter),
    listViews: (filter) => listViews(conn, filter),
    listRoutines: (filter) => listRoutines(conn, filter),
    listTableColumns: (db, table, schema = defaultSchema) => listTableColumns(conn, db, table, schema),
    listTableTriggers: (table, schema = defaultSchema) => listTableTriggers(conn, table, schema),
    listTableIndexes: (db, table, schema = defaultSchema) => listTableIndexes(conn, table, schema),
    listSchemas: (db, filter) => listSchemas(conn, filter),
    getTableReferences: (table, schema = defaultSchema) => getTableReferences(conn, table, schema),
    getTableKeys: (db, table, schema = defaultSchema) => getTableKeys(conn, db, table, schema),
    query: (queryText, schema = defaultSchema) => query(conn, queryText, schema),
    executeQuery: (queryText, schema = defaultSchema) => executeQuery(conn, queryText, schema),
    listDatabases: (filter) => listDatabases(conn, filter),
    getQuerySelectTop: (table, limit, schema = defaultSchema) => getQuerySelectTop(conn, table, limit, schema),
    getTableCreateScript: (table, schema = defaultSchema) => getTableCreateScript(conn, table, schema),
    getViewCreateScript: (view, schema = defaultSchema) => getViewCreateScript(conn, view, schema),
    getRoutineCreateScript: (routine, type, schema = defaultSchema) => getRoutineCreateScript(conn, routine, type, schema),
    truncateAllTables: (_, schema = defaultSchema) => truncateAllTables(conn, schema),
  };
}


export function disconnect(conn) {
  conn.pool.end();
}


export async function listTables(conn, filter) {
  const schemaFilter = buildSchemaFilter(filter, 'table_schema');
  const sql = `
    SELECT
      table_schema as schema,
      table_name as name
    FROM information_schema.tables
    WHERE table_type NOT LIKE '%VIEW%'
    ${schemaFilter ? `AND ${schemaFilter}` : ''}
    ORDER BY table_schema, table_name
  `;

  const data = await driverExecuteQuery(conn, { query: sql });

  return data.rows;
}

export async function listViews(conn, filter) {
  const schemaFilter = buildSchemaFilter(filter, 'table_schema');
  const sql = `
    SELECT
      table_schema as schema,
      table_name as name
    FROM information_schema.views
    ${schemaFilter ? `WHERE ${schemaFilter}` : ''}
    ORDER BY table_schema, table_name
  `;

  const data = await driverExecuteQuery(conn, { query: sql });

  return data.rows;
}

export async function listRoutines(conn, filter) {
  const schemaFilter = buildSchemaFilter(filter, 'routine_schema');
  const sql = `
    SELECT
      routine_schema,
      routine_name,
      routine_type
    FROM information_schema.routines
    ${schemaFilter ? `WHERE ${schemaFilter}` : ''}
    GROUP BY routine_schema, routine_name, routine_type
    ORDER BY routine_schema, routine_name
  `;

  const data = await driverExecuteQuery(conn, { query: sql });

  return data.rows.map((row) => ({
    schema: row.routine_schema,
    routineName: row.routine_name,
    routineType: row.routine_type,
  }));
}

export async function listTableColumns(conn, database, table, schema) {
  const sql = `
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = $1
    AND table_name = $2
    ORDER BY ordinal_position
  `;

  const params = [
    schema,
    table,
  ];

  const data = await driverExecuteQuery(conn, { query: sql, params });

  return data.rows.map((row) => ({
    columnName: row.column_name,
    dataType: row.data_type,
  }));
}

export async function listTableTriggers(conn, table, schema) {
  const sql = `
    SELECT trigger_name
    FROM information_schema.triggers
    WHERE event_object_schema = $1
    AND event_object_table = $2
  `;

  const params = [
    schema,
    table,
  ];

  const data = await driverExecuteQuery(conn, { query: sql, params });

  return data.rows.map((row) => row.trigger_name);
}
export async function listTableIndexes(conn, table, schema) {
  const sql = `
    SELECT indexname as index_name
    FROM pg_indexes
    WHERE schemaname = $1
    AND tablename = $2
  `;

  const params = [
    schema,
    table,
  ];

  const data = await driverExecuteQuery(conn, { query: sql, params });

  return data.rows.map((row) => row.index_name);
}

export async function listSchemas(conn, filter) {
  const schemaFilter = buildSchemaFilter(filter);
  const sql = `
    SELECT schema_name
    FROM information_schema.schemata
    ${schemaFilter ? `WHERE ${schemaFilter}` : ''}
    ORDER BY schema_name
  `;

  const data = await driverExecuteQuery(conn, { query: sql });

  return data.rows.map((row) => row.schema_name);
}

export async function getTableReferences(conn, table, schema) {
  const sql = `
    SELECT ctu.table_name AS referenced_table_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.constraint_table_usage AS ctu
    ON ctu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1
    AND tc.table_schema = $2
  `;

  const params = [
    table,
    schema,
  ];

  const data = await driverExecuteQuery(conn, { query: sql, params });

  return data.rows.map((row) => row.referenced_table_name);
}

export async function getTableKeys(conn, database, table, schema) {
  const sql = `
    SELECT
      tc.constraint_name,
      kcu.column_name,
      CASE WHEN tc.constraint_type LIKE '%FOREIGN%' THEN ctu.table_name
      ELSE NULL
      END AS referenced_table_name,
      tc.constraint_type
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      USING (constraint_schema, constraint_name)
    JOIN information_schema.constraint_table_usage as ctu
      USING (constraint_schema, constraint_name)
    WHERE tc.table_name = $1
    AND tc.table_schema = $2
    AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')

  `;

  const params = [
    table,
    schema,
  ];

  const data = await driverExecuteQuery(conn, { query: sql, params });

  return data.rows.map((row) => ({
    constraintName: row.constraint_name,
    columnName: row.column_name,
    referencedTable: row.referenced_table_name,
    keyType: row.constraint_type,
  }));
}


export function query(conn, queryText) {
  let pid = null;
  let canceling = false;
  const cancelable = createCancelablePromise({
    ...errors.CANCELED_BY_USER,
    sqlectronError: 'CANCELED_BY_USER',
  });

  return {
    execute() {
      return runWithConnection(conn, async (connection) => {
        const connClient = { connection };

        const dataPid = await driverExecuteQuery(connClient, {
          query: 'SELECT pg_backend_pid() AS pid',
        });

        pid = dataPid.rows[0].pid;

        try {
          const data = await Promise.race([
            cancelable.wait(),
            executeQuery(connClient, queryText),
          ]);

          pid = null;
          return data;
        } catch (err) {
          if (canceling && err.code === pgErrors.CANCELED) {
            canceling = false;
            err.sqlectronError = 'CANCELED_BY_USER';
          }

          throw err;
        } finally {
          cancelable.discard();
        }
      });
    },

    async cancel() {
      if (!pid) {
        throw new Error('Query not ready to be canceled');
      }

      canceling = true;
      try {
        const data = await driverExecuteQuery(conn, {
          query: `SELECT pg_cancel_backend(${pid});`,
        });

        if (!data.rows[0].pg_cancel_backend) {
          throw new Error(`Failed canceling query with pid ${pid}.`);
        }

        cancelable.cancel();
      } catch (err) {
        canceling = false;
        throw err;
      }
    },
  };
}


export async function executeQuery(conn, queryText) {
  const commands = identifyCommands(queryText).map((item) => item.type);

  let data = await driverExecuteQuery(conn, { query: queryText, multiple: true });
  if (!Array.isArray(data)) {
    data = [data];
  }

  return data.filter((result) => result.command !== null).map((result, idx) => parseRowQueryResult(result, commands[idx]));
}


export async function listDatabases(conn, filter) {
  const databaseFilter = buildDatabseFilter(filter, 'datname');
  const sql = `
    SELECT datname
    FROM pg_database
    WHERE datistemplate = $1
    ${databaseFilter ? `AND ${databaseFilter}` : ''}
    ORDER BY datname
  `;

  const params = [false];

  const data = await driverExecuteQuery(conn, { query: sql, params });

  return data.rows.map((row) => row.datname);
}


export function getQuerySelectTop(conn, table, limit, schema) {
  return `SELECT * FROM ${wrapIdentifier(schema)}.${wrapIdentifier(table)} LIMIT ${limit}`;
}

export async function getTableCreateScript(conn, table, schema) {
  // Reference http://stackoverflow.com/a/32885178

  const params = [
    table,
    schema,
  ];

  const tableSql = `
    SELECT
      'CREATE TABLE ' || quote_ident(n.nspname) || '.' || quote_ident(c.relname) || ' (' AS create_table
    FROM pg_class c,
    pg_namespace n
    WHERE c.relname = $1
    AND n.nspname = $2
  `;
  let createTable = (await driverExecuteQuery(conn, { query: tableSql, params })).rows[0].create_table;

  const columnSql = `
    SELECT
      quote_ident(a.attname) AS column_name,
      pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
      CASE
        WHEN a.attnotnull THEN 'NOT NULL'
      ELSE 'NULL'
      END AS not_null,
      a.attnum as column_order
    FROM pg_class c,
    pg_attribute a,
    pg_type t,
    pg_namespace n
    WHERE c.relname = $1
    AND a.attnum > 0
    AND a.attrelid = c.oid
    AND a.atttypid = t.oid
    AND n.oid = c.relnamespace
    AND n.nspname = $2
    ORDER BY a.attnum ASC
  `;
  const columnData = await driverExecuteQuery(conn, { query: columnSql, params });

  const columns = [];
  columnData.rows.forEach((row) => {
    columns.push(`  ${row.column_name} ${row.type} ${row.not_null}`);
  });

  createTable += `\n${columns.join(',\n')}\n);\n`;

  const constraintSql = `
    SELECT
      CASE WHEN tc.constraint_name IS NULL THEN ''
              ELSE 'ALTER TABLE ' || quote_ident($2) || '.' || quote_ident($1) ||
              ' ADD CONSTRAINT ' || quote_ident(tc.constraint_name)  ||
              ' PRIMARY KEY ' || '(' || constr.column_name || ')'
          END AS constraint
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.key_column_usage as constr
    ON constr.constraint_name = tc.constraint_name
    AND constr.table_name = tc.table_name
    AND constr.table_schema = tc.table_schema
    WHERE
      tc.constraint_type  = 'PRIMARY KEY'
      AND tc.table_name   = $1
      AND tc.table_schema = $2;
  `;

  const constraintResult = (await driverExecuteQuery(conn, { query: constraintSql, params })).rows[0];
  if (constraintResult.constraint.length > 0) {
    createTable += `\n${constraintResult.constraint}`;
  }
  return [createTable];
}

export async function getViewCreateScript(conn, view, schema) {
  const createViewSql = `CREATE OR REPLACE VIEW ${wrapIdentifier(schema)}.${view} AS`;

  const sql = 'SELECT pg_get_viewdef($1::regclass, true)';

  const params = [view];

  const data = await driverExecuteQuery(conn, { query: sql, params });

  return data.rows.map((row) => `${createViewSql}\n${row.pg_get_viewdef}`);
}

export async function getRoutineCreateScript(conn, routine, _, schema) {
  let mapFunction;
  let sql;
  if (versionCompare(conn.versionData.version, '8.4') >= 0) {
    sql = `
      SELECT pg_get_functiondef(p.oid)
      FROM pg_proc p
      LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
      WHERE proname = $1
      AND n.nspname = $2
    `;
    mapFunction = (row) => row.pg_get_functiondef;
  } else {
    // -- pg_catalog.array_to_string(p.proacl, '\n') AS "Access privileges",
    sql = `
      SELECT
        p.proname,
        n.nspname,
        CASE
          WHEN p.proretset THEN 'SETOF '
          ELSE ''
        END || pg_catalog.format_type(p.prorettype, NULL) as prorettype,
        p.proargnames,
        pg_catalog.oidvectortypes(p.proargtypes) as "proargtypes",
        CASE
          WHEN p.proisagg THEN 'agg'
          WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
          ELSE 'func'
        END AS protype,
        CASE
          WHEN p.provolatile = 'i' THEN 'IMMUTABLE'
          WHEN p.provolatile = 's' THEN 'STABLE'
          WHEN p.provolatile = 'v' THEN 'VOLATILE'
        END as provolatility,
        pg_catalog.pg_get_userbyid(p.proowner) as prowner_name,
        CASE WHEN prosecdef THEN 'definer' ELSE 'invoker' END AS prosecurity,
        l.lanname,
        p.prosrc,
        pg_catalog.obj_description(p.oid, 'pg_proc') as "description"
      FROM pg_catalog.pg_proc p
          LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
          LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang
      WHERE p.proname = $1
        AND n.nspname = $2
        AND pg_catalog.pg_function_is_visible(p.oid);
    `;
    mapFunction = (row) => {
      // TODO: expand support for other types as necessary
      if (row.protype !== 'func') {
        return row.prosrc;
      }

      let args = '';
      if (row.proargtypes && row.proargtypes.length > 0) {
        args = (row.proargtypes || '').split(', ').map((val, idx) => `${row.proargnames[idx]} ${val}`).join(', ');
      }
      return `CREATE OR REPLACE FUNCTION ${row.nspname}.${row.proname}(${args})\n  RETURNS ${row.prorettype} AS $$${row.prosrc}$$ LANGUAGE ${row.lanname} ${row.provolatility}`;
    };
  }

  const params = [
    routine,
    schema,
  ];

  const data = await driverExecuteQuery(conn, { query: sql, params });

  return data.rows.map(mapFunction);
}

export function wrapIdentifier(value) {
  if (value === '*') return value;
  const matched = value.match(/(.*?)(\[[0-9]\])/); // eslint-disable-line no-useless-escape
  if (matched) return wrapIdentifier(matched[1]) + matched[2];
  return `"${value.replace(/"/g, '""')}"`;
}


async function getSchema(conn) {
  const sql = 'SELECT current_schema() AS schema';

  const data = await driverExecuteQuery(conn, { query: sql });

  return data.rows[0].schema;
}

export async function truncateAllTables(conn, schema) {
  await runWithConnection(conn, async (connection) => {
    const connClient = { connection };

    const sql = `
      SELECT quote_ident(table_name) as table_name
      FROM information_schema.tables
      WHERE table_schema = $1
      AND table_type NOT LIKE '%VIEW%'
    `;

    const params = [
      schema,
    ];

    const data = await driverExecuteQuery(connClient, { query: sql, params });

    if (versionCompare(conn.versionData.version, '8.4') >= 0) {
      const truncateAll = data.rows.map((row) => `
        TRUNCATE TABLE ${wrapIdentifier(schema)}.${wrapIdentifier(row.table_name)}
        RESTART IDENTITY CASCADE;
      `).join('');

      await driverExecuteQuery(connClient, { query: truncateAll, multiple: true });
    } else {
      // RESTART IDENTITY CASCADE was added in Postgres 8.4. The cascade handled
      // under the hood the foreign key constraints so without it, we first have
      // to remove all of them, run the truncate, reset the sequences, and then
      // readd the constraints.
      const seqData = await driverExecuteQuery(connClient, {
        query: "SELECT relname FROM pg_class WHERE relkind = 'S'",
      });
      const disableTriggers = await driverExecuteQuery(connClient, {
        query: `
          SELECT 'ALTER TABLE '||quote_ident(nspname)||'.'||quote_ident(relname)||' DROP CONSTRAINT '||quote_ident(conname)||';' AS query
          FROM pg_constraint
          INNER JOIN pg_class ON conrelid=pg_class.oid
          INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
          WHERE contype='f'
          ORDER BY nspname, relname, conname;
        `,
      });
      const enableTriggers = await driverExecuteQuery(connClient, {
        query: `
          SELECT 'ALTER TABLE "'||nspname||'"."'||relname||'" ADD CONSTRAINT "'||conname||'" '|| pg_get_constraintdef(pg_constraint.oid)||';' AS query
          FROM pg_constraint
          INNER JOIN pg_class ON conrelid=pg_class.oid
          INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
          WHERE contype='f'
          ORDER BY nspname DESC,relname DESC,conname DESC;
        `,
      });

      await driverExecuteQuery(connClient, { query: 'BEGIN' });

      let truncateAll = '';
      truncateAll += disableTriggers.rows.map((row) => `${row.query}`).join('\n');
      truncateAll += data.rows.map((row) => `
        TRUNCATE TABLE ${wrapIdentifier(schema)}.${wrapIdentifier(row.table_name)};
      `).join('');
      truncateAll += seqData.rows.map((row) => `
        ALTER SEQUENCE ${row.relname} START 1;
      `).join('');
      truncateAll += enableTriggers.rows.map((row) => `${row.query}`).join('\n');

      try {
        await driverExecuteQuery(connClient, { query: truncateAll, multiple: true });
        await driverExecuteQuery(connClient, { query: 'COMMIT' });
      } catch (e) {
        await driverExecuteQuery(connClient, { query: 'ROLLBACK' });
        throw e;
      }
    }
  });
}

function configDatabase(server, database) {
  const config = {
    host: server.config.host,
    port: server.config.port,
    user: server.config.user,
    password: server.config.password,
    database: database.database,
    max: 5, // max idle connections per time (30 secs)
  };

  if (server.sshTunnel) {
    config.host = server.config.localHost;
    config.port = server.config.localPort;
  }

  if (server.config.ssl) {
    config.ssl = server.config.ssl;
  }

  return config;
}


function parseRowQueryResult(data, command) {
  const isSelect = data.command === 'SELECT';
  return {
    command: command || data.command,
    rows: data.rows,
    fields: data.fields,
    rowCount: isSelect ? (data.rowCount || data.rows.length) : undefined,
    affectedRows: !isSelect && !isNaN(data.rowCount) && data.rowCount !== null ? data.rowCount : undefined,
  };
}


function identifyCommands(queryText) {
  try {
    return identify(queryText);
  } catch (err) {
    return [];
  }
}

function driverExecuteQuery(conn, queryArgs) {
  const runQuery = (connection) => {
    const args = {
      text: queryArgs.query,
      values: queryArgs.params,
      multiResult: queryArgs.multiple,
    };

    // node-postgres has support for Promise query
    // but that always returns the "fields" property empty
    return new Promise((resolve, reject) => {
      connection.query(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  return conn.connection
    ? runQuery(conn.connection)
    : runWithConnection(conn, runQuery);
}

async function runWithConnection({ pool }, run) {
  const connection = await pool.connect();

  try {
    return await run(connection);
  } finally {
    connection.release();
  }
}
