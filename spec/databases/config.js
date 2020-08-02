import path from 'path';
import { ConnectionString } from 'connection-string';

const dbs = {
  sqlite: {
    database: path.join(__dirname, 'sqlite', 'sqlectron.db'),
  },
};

if (process.env.POSTGRES_DSN) {
  const postgres = new ConnectionString(process.env.POSTGRES_DSN);
  dbs.postgresql = {
    host: postgres.hosts[0].name,
    port: postgres.hosts[0].port || 5432,
    user: postgres.user || 'postgres',
    password: postgres.password || '',
    database: postgres.params.dbname || 'sqlectron',
  };
}

if (process.env.MYSQL_DSN) {
  const mysql = new ConnectionString(process.env.MYSQL_DSN);
  dbs.mysql = {
    host: mysql.hosts[0].name,
    port: mysql.hosts[0].port || 3306,
    user: mysql.user || 'root',
    password: mysql.password || '',
    database: mysql.params.dbname || 'sqlectron',
  };
}

if (process.env.MARIADB_DSN) {
  const mariadb = new ConnectionString(process.env.MARIADB_DSN);
  dbs.mariadb = {
    host: mariadb.hosts[0].name,
    port: mariadb.hosts[0].port || 3306,
    user: mariadb.user || 'root',
    password: mariadb.password || '',
    database: mariadb.params.dbname || 'sqlectron',
  };
}

if (process.env.SQLSERVER_DSN) {
  const sqlserver = new ConnectionString(process.env.SQLSERVER_DSN);
  dbs.sqlserver = {
    host: sqlserver.hosts[0].name,
    port: sqlserver.hosts[0].port || 1433,
    user: sqlserver.user || 'sa',
    password: sqlserver.password || '',
    database: sqlserver.params.dbname || 'sqlectron,',
  };
}

if (process.env.CASSANDRA_DSN) {
  const cassandra = new ConnectionString(process.env.CASSANDRA_DSN);
  dbs.cassandra = {
    host: cassandra.hosts[0].name,
    port: cassandra.hosts[0].port || 9042,
    database: cassandra.params ? cassandra.params.dbname || 'sqlectron' : 'sqlectron',
  };
}

export default dbs;
