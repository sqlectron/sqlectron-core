import url from 'url';
import path from 'path';

export default {
  postgresql: {
    host: url.parse(process.env.POSTGRES_PORT || '').hostname,
    port: parseInt(url.parse(process.env.POSTGRES_PORT || '').port, 10),
    user: process.env.POSTGRES_ENV_POSTGRES_USER,
    password: process.env.POSTGRES_ENV_POSTGRES_PASSWORD,
    database: process.env.POSTGRES_ENV_POSTGRES_DB,
  },
  mysql: {
    host: url.parse(process.env.MYSQL_PORT || '').hostname,
    port: parseInt(url.parse(process.env.MYSQL_PORT || '').port, 10),
    user: process.env.MYSQL_ENV_MYSQL_USER,
    password: process.env.MYSQL_ENV_MYSQL_PASSWORD,
    database: process.env.MYSQL_ENV_MYSQL_DATABASE,
    multipleStatements: true,
  },
  mariadb: {
    host: url.parse(process.env.MARIADB_PORT || '').hostname,
    port: parseInt(url.parse(process.env.MARIADB_PORT || '').port, 10),
    user: process.env.MARIADB_ENV_MARIADB_USER,
    password: process.env.MARIADB_ENV_MARIADB_PASSWORD,
    database: process.env.MARIADB_ENV_MARIADB_DATABASE,
    multipleStatements: true,
  },
  sqlserver: {
    host: process.env.SQLSERVER_ENV_SQLSERVER_HOST,
    port: parseInt(process.env.SQLSERVER_ENV_SQLSERVER_PORT, 10),
    user: process.env.SQLSERVER_ENV_SQLSERVER_USER,
    password: process.env.SQLSERVER_ENV_SQLSERVER_PASSWORD,
    database: process.env.SQLSERVER_ENV_SQLSERVER_DATABASE,
  },
  sqlite: {
    database: path.join(__dirname, 'sqlite', 'sqlectron.db'),
  },
  cassandra: {
    host: url.parse(process.env.CASSANDRA_PORT || '').hostname,
    port: 9042,
    database: 'sqlectron',
  },
};
