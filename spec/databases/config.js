import path from 'path';
import { ConnectionString } from 'connection-string';

const dbs = {
  sqlite: {
    database: path.join(__dirname, 'sqlite', 'sqlectron.db'),
  },
};

const postgres = new ConnectionString(process.env.POSTGRES_DSN, {
  protocol: 'postgres',
  user: 'postgres',
  password: 'Password12!',
  path: ['sqlectron'],
  hosts: [{ name: 'localhost', port: 5432 }],
});
dbs.postgresql = {
  host: postgres.hostname,
  port: postgres.port || 5432,
  user: postgres.user,
  password: postgres.password,
  database: postgres.path && postgres.path[0],
};

const mysql = new ConnectionString(process.env.MYSQL_DSN, {
  protocol: 'mysql',
  user: 'root',
  password: 'Password12!',
  path: ['sqlectron'],
  hosts: [{ name: 'localhost', port: 3306 }],
});
dbs.mysql = {
  host: mysql.hostname,
  port: mysql.port || 3306,
  user: mysql.user,
  password: mysql.password,
  database: mysql.path && mysql.path[0],
};

const mariadb = new ConnectionString(process.env.MARIADB_DSN, {
  user: 'root',
  password: 'Password12!',
  path: ['sqlectron'],
  hosts: [{ name: 'localhost', port: 3307 }],
});
dbs.mariadb = {
  host: mariadb.hostname,
  port: mariadb.port || 3307,
  user: mariadb.user,
  password: mariadb.password,
  database: mariadb.path && mariadb.path[0],
};

const sqlserver = new ConnectionString(process.env.SQLSERVER_DSN, {
  protocol: 'mssql',
  user: 'sa',
  password: 'Password12!',
  path: ['sqlectron'],
  hosts: [{ name: 'localhost', port: 1433 }],
});
dbs.sqlserver = {
  host: sqlserver.hostname,
  port: sqlserver.port || 1433,
  user: sqlserver.user,
  password: sqlserver.password,
  database: sqlserver.path && sqlserver.path[0],
};

const cassandra = new ConnectionString(process.env.CASSANDRA_DSN, {
  protocol: 'cassandra',
  path: ['sqlectron'],
  hosts: [{ name: 'localhost', port: 9042 }],
});
dbs.cassandra = {
  host: cassandra.hostname,
  port: cassandra.port || 9042,
  database: cassandra.path && cassandra.path[0],
};

export default dbs;
