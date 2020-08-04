import path from 'path';
import { ConnectionString } from 'connection-string';

const dbs = {
  sqlite: {
    database: path.join(__dirname, 'sqlite', 'sqlectron.db'),
  },
};

if (process.env.POSTGRES_DSN) {
  const postgres = new ConnectionString(process.env.POSTGRES_DSN, {
    user: 'postgres',
    password: '',
    database: 'sqlectron',
  });
  dbs.postgresql = {
    host: postgres.hostname,
    port: postgres.port || 5432,
    user: postgres.user,
    password: postgres.password,
    database: postgres.path && postgres.path[0],
  };
}

if (process.env.MYSQL_DSN) {
  const mysql = new ConnectionString(process.env.MYSQL_DSN, {
    user: 'root',
    password: '',
    database: 'sqlectron',
  });
  dbs.mysql = {
    host: mysql.hostname,
    port: mysql.port || 3306,
    user: mysql.user,
    password: mysql.password,
    database: mysql.path && mysql.path[0],
  };
}

if (process.env.MARIADB_DSN) {
  const mariadb = new ConnectionString(process.env.MARIADB_DSN, {
    user: 'root',
    password: '',
    database: 'sqlectron',
  });
  dbs.mariadb = {
    host: mariadb.hostname,
    port: mariadb.port || 3306,
    user: mariadb.user,
    password: mariadb.password,
    database: mariadb.path && mariadb.path[0],
  };
}

if (process.env.SQLSERVER_DSN) {
  const sqlserver = new ConnectionString(process.env.SQLSERVER_DSN, {
    user: 'sa',
    password: '',
    database: 'sqlectron',
  });
  dbs.sqlserver = {
    host: sqlserver.hostname,
    port: sqlserver.port || 1433,
    user: sqlserver.user,
    password: sqlserver.password,
    database: sqlserver.path && sqlserver.path[0],
  };
}

if (process.env.CASSANDRA_DSN) {
  const cassandra = new ConnectionString(process.env.CASSANDRA_DSN, {
    database: 'sqlectron',
  });
  dbs.cassandra = {
    host: cassandra.hostname,
    port: cassandra.port || 9042,
    database: cassandra.path && cassandra.path[0],
  };
}

export default dbs;
