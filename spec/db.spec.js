import chai, { expect } from 'chai';
import chaiAsPromised from 'chai-as-promised';
import { stub } from 'sinon';
import config from './databases/config';
import setupSQLite from './databases/sqlite/setup';
import setupCassandra from './databases/cassandra/setup';
import { db, config as srcConfig } from '../src';
import { clearLimitSelect } from '../src/db/client';
import { versionCompare } from '../src/utils';

chai.use(chaiAsPromised);

/**
 * List of supported DB clients.
 * The "integration" tests will be executed for all supported DB clients.
 * And ensure all these clients has the same API and output results.
 */
const SUPPORTED_DB_CLIENTS = db.CLIENTS.map((client) => client.key);

const dbSchemas = {
  redshift: 'public',
  postgresql: 'public',
  sqlserver: 'dbo',
};

/**
 * List of selected databases to be tested in the current task
 */
const dbsToTest = (process.env.DB_CLIENTS || '').split(',').filter((client) => !!client);

const postgresClients = ['postgresql', 'redshift'];
const mysqlClients = ['mysql', 'mariadb'];

describe('db', () => {
  const dbClients = dbsToTest.length ? dbsToTest : SUPPORTED_DB_CLIENTS;
  if (dbClients.some((dbClient) => !~SUPPORTED_DB_CLIENTS.indexOf(dbClient))) {
    throw new Error('Invalid selected db client for tests');
  }

  if (dbClients.includes('sqlite')) {
    setupSQLite(config.sqlite);
  }
  if (dbClients.includes('cassandra')) {
    setupCassandra(config.cassandra);
  }

  dbClients.forEach((dbClient) => {
    const dbSchema = dbSchemas[dbClient];

    describe(dbClient, () => {
      describe('.connect', () => {
        it(`should connect into a ${dbClient} database`, () => {
          const serverInfo = {
            ...config[dbClient],
            name: dbClient,
            client: dbClient,
          };

          const serverSession = db.createServer(serverInfo);
          const dbConn = serverSession.createConnection(serverInfo.database);

          return expect(dbConn.connect()).to.not.be.rejected;
        });

        it('should connect into server without database specified', () => {
          const serverInfo = {
            ...config[dbClient],
            database: db.CLIENTS.find((c) => c.key === dbClient).defaultDatabase,
            name: dbClient,
            client: dbClient,
          };

          const serverSession = db.createServer(serverInfo);
          const dbConn = serverSession.createConnection(serverInfo.database);

          return expect(dbConn.connect()).to.not.be.rejected;
        });
      });

      describe('given is already connected', () => {
        const serverInfo = {
          ...config[dbClient],
          name: dbClient,
          client: dbClient,
        };

        let serverSession;
        let dbConn;
        beforeEach(() => {
          serverSession = db.createServer(serverInfo);
          dbConn = serverSession.createConnection(serverInfo.database);
          return dbConn.connect();
        });

        describe('.disconnect', () => {
          it('should close all connections in the pool', () => {
            dbConn.disconnect();
          });
        });

        describe('.version', () => {
          it('should return version string', () => {
            const version = dbConn.version();
            expect(version).to.be.a('string').and.not.be.empty;
          });
        });

        describe('.getVersion', () => {
          it('should return version details', () => {
            const version = dbConn.getVersion();
            expect(dbConn.getVersion()).to.be.a('object');
            const expectedName = {
              postgresql: 'PostgreSQL',
              redshift: 'PostgreSQL', // redshift does not identify itself uniquely from postgres 8
              mysql: 'MySQL',
              mariadb: 'MariaDB',
              sqlite: 'SQLite',
              sqlserver: 'SQL Server',
              cassandra: 'Cassandra',
            };
            expect(version).to.have.property('name').to.contain(expectedName[dbClient]);
            expect(version).to.have.property('version').to.be.a('string').and.to.match(/(?:[0-9]\.?)+/);
            expect(version).to.have.property('string').to.be.a('string').and.to.be.not.empty;
          });
        });

        describe('.listDatabases', () => {
          it('should list all databases', async () => {
            const databases = await dbConn.listDatabases();
            if (dbClient === 'sqlite') {
              expect(databases[0]).to.match(/sqlectron\.db$/);
            } else {
              expect(databases).to.include.members(['sqlectron']);
            }
          });
        });

        describe('.listTables', () => {
          it('should list all tables', async () => {
            const tables = await dbConn.listTables({ schema: dbSchema });
            if (postgresClients.includes(dbClient) || dbClient === 'sqlserver') {
              expect(tables).to.eql([
                { schema: dbSchema, name: 'roles' },
                { schema: dbSchema, name: 'users' },
              ]);
            } else {
              expect(tables).to.eql([
                { name: 'roles' },
                { name: 'users' },
              ]);
            }
          });
        });

        if (dbClient !== 'cassandra') {
          describe('.listViews', () => {
            it('should list all views', async () => {
              const views = await dbConn.listViews({ schema: dbSchema });
              if (postgresClients.includes(dbClient) || dbClient === 'sqlserver') {
                expect(views).to.eql([
                  { schema: dbSchema, name: 'email_view' },
                ]);
              } else {
                expect(views).to.eql([
                  { name: 'email_view' },
                ]);
              }
            });
          });
        }

        describe('.listRoutines', () => {
          it('should list all routines with their type', async () => {
            const routines = await dbConn.listRoutines({ schema: dbSchema });
            const routine = dbClient === 'postgresql' ? routines[1] : routines[0];

            // Postgresql routine type is always function. SP do not exist
            // Futhermore, PostgreSQL is expected to have two functions in schema, because
            // additional one is needed for trigger
            if (dbClient === 'postgresql') {
              expect(routines).to.have.length(2);
              expect(routine).to.have.deep.property('routineType').to.eql('FUNCTION');
              expect(routine).to.have.deep.property('schema').to.eql(dbSchema);
            } else if (dbClient === 'redshift') {
              expect(routines).to.have.length(1);
              expect(routine).to.have.deep.property('routineType').to.eql('FUNCTION');
              expect(routine).to.have.deep.property('schema').to.eql(dbSchema);
            } else if (mysqlClients.includes(dbClient)) {
              expect(routines).to.have.length(1);
              expect(routine).to.have.deep.property('routineType').to.eql('PROCEDURE');
              expect(routine).to.not.have.deep.property('schema');
            } else if (dbClient === 'sqlserver') {
              expect(routines).to.have.length(1);
              expect(routine).to.have.deep.property('routineType').to.eql('PROCEDURE');
              expect(routine).to.have.deep.property('schema').to.eql(dbSchema);
            } else if (dbClient === 'cassandra' || dbClient === 'sqlite') {
              expect(routines).to.have.length(0);
            } else {
              throw new Error('Invalid db client');
            }
          });
        });

        describe('.listTableColumns', () => {
          it('should list all columns and their type from users table', async () => {
            const columns = await dbConn.listTableColumns('users');
            expect(columns).to.have.length(6);

            const column = (name) => columns.find((col) => col.columnName === name);

            /* eslint no-unused-expressions:0 */
            expect(column('id')).to.exist;
            expect(column('username')).to.exist;
            expect(column('email')).to.exist;
            expect(column('password')).to.exist;
            expect(column('role_id')).to.exist;
            expect(column('createdat')).to.exist;

            if (dbClient === 'sqlite') {
              expect(column('id')).to.have.property('dataType').to.have.string('INTEGER');
            } else {
              expect(column('id')).to.have.property('dataType').to.have.string('int');
            }

            // Each database may have different db types
            if (postgresClients.includes(dbClient)) {
              expect(column('username')).to.have.property('dataType').to.eql('text');
              expect(column('email')).to.have.property('dataType').to.eql('text');
              expect(column('password')).to.have.property('dataType').to.eql('text');
              expect(column('role_id')).to.have.property('dataType').to.eql('integer');
              expect(column('createdat')).to.have.property('dataType').to.eql('date');
            } else if (dbClient === 'sqlite') {
              expect(column('username')).to.have.property('dataType').to.eql('VARCHAR(45)');
              expect(column('email')).to.have.property('dataType').to.eql('VARCHAR(150)');
              expect(column('password')).to.have.property('dataType').to.eql('VARCHAR(45)');
              expect(column('role_id')).to.have.property('dataType').to.eql('INT');
              expect(column('createdat')).to.have.property('dataType').to.eql('DATETIME');
            } else if (dbClient === 'cassandra') {
              expect(column('username')).to.have.property('dataType').to.eql('text');
              expect(column('email')).to.have.property('dataType').to.eql('text');
              expect(column('password')).to.have.property('dataType').to.eql('text');
              expect(column('role_id')).to.have.property('dataType').to.eql('int');
              expect(column('createdat')).to.have.property('dataType').to.eql('timestamp');
            } else {
              expect(column('username')).to.have.property('dataType').to.eql('varchar');
              expect(column('email')).to.have.property('dataType').to.eql('varchar');
              expect(column('password')).to.have.property('dataType').to.eql('varchar');
              expect(column('role_id')).to.have.property('dataType').to.eql('int');
              expect(column('createdat')).to.have.property('dataType').to.eql('datetime');
            }
          });
        });

        describe('.listTableTriggers', () => {
          it('should list all table related triggers', async () => {
            const triggers = await dbConn.listTableTriggers('users');
            if (dbClient === 'cassandra' || dbClient === 'redshift') {
              expect(triggers).to.have.length(0);
            } else {
              expect(triggers).to.have.length(1);
              expect(triggers).to.include.members(['dummy_trigger']);
            }
          });
        });

        describe('.listTableIndexes', () => {
          it('should list all indexes', async () => {
            const indexes = await dbConn.listTableIndexes('users', dbSchema);
            if (dbClient === 'cassandra') {
              expect(indexes).to.have.length(0);
            } else if (dbClient === 'sqlite') {
              expect(indexes).to.have.length(1);
              expect(indexes).to.include.members(['users_id_index']);
            } else if (postgresClients.includes(dbClient)) {
              expect(indexes).to.have.length(1);
              expect(indexes).to.include.members(['users_pkey']);
            } else if (mysqlClients.includes(dbClient)) {
              expect(indexes).to.have.length(2);
              expect(indexes).to.include.members(['PRIMARY', 'role_id']);
            } else if (dbClient === 'sqlserver') {
              expect(indexes).to.have.length(1);
              expect(indexes[0]).to.match(/^PK__users__/i);
            } else {
              throw new Error('Invalid db client');
            }
          });
        });

        describe('.listSchemas', () => {
          it('should list all schema', async () => {
            const schemas = await dbConn.listSchemas({ schema: { only: [dbSchema, 'dummy_schema'] } });
            if (postgresClients.includes(dbClient)) {
              expect(schemas).to.have.length(2);
              expect(schemas).to.include.members([dbSchema, 'dummy_schema']);
            } else if (dbClient === 'sqlserver') {
              expect(schemas).to.include('dummy_schema');
            } else {
              expect(schemas).to.have.length(0);
            }
          });
        });

        describe('.getTableReferences', () => {
          it('should list all tables that selected table has references to', async () => {
            const references = await dbConn.getTableReferences('users');
            if (dbClient === 'cassandra' || dbClient === 'sqlite') {
              expect(references).to.have.length(0);
            } else {
              expect(references).to.have.length(1);
              expect(references).to.include.members(['roles']);
            }
          });
        });

        describe('.getTableKeys', () => {
          it('should list all tables keys', async () => {
            const tableKeys = await dbConn.getTableKeys('users');
            if (dbClient === 'cassandra') {
              expect(tableKeys).to.have.length(1);
            } else if (dbClient === 'sqlite') {
              expect(tableKeys).to.have.length(0);
            } else {
              expect(tableKeys).to.have.length(2);
            }

            tableKeys.forEach((key) => {
              if (key.keyType === 'PRIMARY KEY') {
                expect(key).to.have.property('columnName').to.eql('id');
                expect(key).to.have.property('referencedTable').to.be.a('null');
              } else {
                expect(key).to.have.property('columnName').to.eql('role_id');
                expect(key).to.have.property('referencedTable').to.eql('roles');
                expect(key).to.have.property('keyType').to.eql('FOREIGN KEY');
              }
            });
          });
        });

        describe('.getTableCreateScript', () => {
          it('should return table create script', async () => {
            const [createScript] = await dbConn.getTableCreateScript('users');

            if (dbClient === 'mysql' && versionCompare(dbConn.getVersion().version, '8') >= 0) {
              expect(createScript).to.contain('CREATE TABLE `users` (\n' +
              '  `id` int NOT NULL AUTO_INCREMENT,\n' +
              '  `username` varchar(45) DEFAULT NULL,\n' +
              '  `email` varchar(150) DEFAULT NULL,\n' +
              '  `password` varchar(45) DEFAULT NULL,\n' +
              '  `role_id` int DEFAULT NULL,\n' +
              '  `createdat` datetime DEFAULT NULL,\n' +
              '  PRIMARY KEY (`id`),\n' +
              '  KEY `role_id` (`role_id`),\n' +
              '  CONSTRAINT `users_ibfk_1` FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON DELETE CASCADE\n' +
              ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci');
            } else if (mysqlClients.includes(dbClient)) {
              expect(createScript).to.contain('CREATE TABLE `users` (\n' +
                '  `id` int(11) NOT NULL AUTO_INCREMENT,\n' +
                '  `username` varchar(45) DEFAULT NULL,\n' +
                '  `email` varchar(150) DEFAULT NULL,\n' +
                '  `password` varchar(45) DEFAULT NULL,\n' +
                '  `role_id` int(11) DEFAULT NULL,\n' +
                '  `createdat` datetime DEFAULT NULL,\n' +
                '  PRIMARY KEY (`id`),\n' +
                '  KEY `role_id` (`role_id`),\n' +
                '  CONSTRAINT `users_ibfk_1` FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON DELETE CASCADE\n' +
              ') ENGINE=InnoDB');
            } else if (postgresClients.includes(dbClient)) {
              expect(createScript).to.eql('CREATE TABLE public.users (\n' +
                '  id integer NOT NULL,\n' +
                '  username text NOT NULL,\n' +
                '  email text NOT NULL,\n' +
                `  ${dbClient === 'postgresql' ? 'password' : '"password"'} text NOT NULL,\n` +
                '  role_id integer NULL,\n' +
                '  createdat date NULL\n' +
                ');\n' +
                '\n' +
                'ALTER TABLE public.users ADD CONSTRAINT users_pkey PRIMARY KEY (id)',
              );
            } else if (dbClient === 'sqlserver') {
              expect(createScript).to.contain('CREATE TABLE users (\r\n' +
                '  id int IDENTITY(1,1) NOT NULL,\r\n' +
                '  username varchar(45)  NULL,\r\n' +
                '  email varchar(150)  NULL,\r\n' +
                '  password varchar(45)  NULL,\r\n' +
                '  role_id int  NULL,\r\n' +
                '  createdat datetime  NULL,\r\n' +
                ')\r\n');
              expect(createScript).to.contain('ALTER TABLE users ADD CONSTRAINT PK__users');
              expect(createScript).to.contain('PRIMARY KEY (id)');
            } else if (dbClient === 'sqlite') {
              expect(createScript).to.eql('CREATE TABLE users (\n' +
                '  id INTEGER NOT NULL,\n' +
                '  username VARCHAR(45) NULL,\n' +
                '  email VARCHAR(150) NULL,\n' +
                '  password VARCHAR(45) NULL,\n' +
                '  role_id INT,\n' +
                '  createdat DATETIME NULL,\n' +
                '  PRIMARY KEY (id),\n' +
                '  FOREIGN KEY (role_id) REFERENCES roles (id)\n)',
              );
            } else if (dbClient === 'cassandra') {
              expect(createScript).to.eql(undefined);
            } else {
              throw new Error('Invalid db client');
            }
          });
        });

        describe('.getTableSelectScript', () => {
          it('should return SELECT table script', async () => {
            const selectQuery = await dbConn.getTableSelectScript('users');
            if (mysqlClients.includes(dbClient)) {
              expect(selectQuery).to.eql('SELECT `id`, `username`, `email`, `password`, `role_id`, `createdat` FROM `users`;');
            } else if (dbClient === 'sqlserver') {
              expect(selectQuery).to.eql('SELECT [id], [username], [email], [password], [role_id], [createdat] FROM [users];');
            } else if (postgresClients.includes(dbClient) || dbClient === 'sqlite') {
              expect(selectQuery).to.eql('SELECT "id", "username", "email", "password", "role_id", "createdat" FROM "users";');
            } else if (dbClient === 'cassandra') {
              expect(selectQuery).to.eql('SELECT "id", "createdat", "email", "password", "role_id", "username" FROM "users";');
            } else {
              throw new Error('Invalid db client');
            }
          });

          it('should return SELECT table script with schema if defined', async () => {
            const selectQuery = await dbConn.getTableSelectScript('users', 'public');
            if (dbClient === 'sqlserver') {
              expect(selectQuery).to.eql('SELECT [id], [username], [email], [password], [role_id], [createdat] FROM [public].[users];');
            } else if (postgresClients.includes(dbClient)) {
              expect(selectQuery).to.eql('SELECT "id", "username", "email", "password", "role_id", "createdat" FROM "public"."users";');
            }
          });
        });


        describe('.getTableInsertScript', () => {
          it('should return INSERT INTO table script', async () => {
            const insertQuery = await dbConn.getTableInsertScript('users');
            if (mysqlClients.includes(dbClient)) {
              expect(insertQuery).to.eql([
                'INSERT INTO `users` (`id`, `username`, `email`, `password`, `role_id`, `createdat`)\n',
                'VALUES (?, ?, ?, ?, ?, ?);',
              ].join(' '));
            } else if (dbClient === 'sqlserver') {
              expect(insertQuery).to.eql([
                'INSERT INTO [users] ([id], [username], [email], [password], [role_id], [createdat])\n',
                'VALUES (?, ?, ?, ?, ?, ?);',
              ].join(' '));
            } else if (postgresClients.includes(dbClient) || dbClient === 'sqlite') {
              expect(insertQuery).to.eql([
                'INSERT INTO "users" ("id", "username", "email", "password", "role_id", "createdat")\n',
                'VALUES (?, ?, ?, ?, ?, ?);',
              ].join(' '));
            } else if (dbClient === 'cassandra') {
              expect(insertQuery).to.eql([
                'INSERT INTO "users" ("id", "createdat", "email", "password", "role_id", "username")\n',
                'VALUES (?, ?, ?, ?, ?, ?);',
              ].join(' '));
            } else {
              throw new Error('Invalid db client');
            }
          });

          it('should return INSERT INTO table script with schema if defined', async () => {
            const insertQuery = await dbConn.getTableInsertScript('users', 'public');
            if (dbClient === 'sqlserver') {
              expect(insertQuery).to.eql([
                'INSERT INTO [public].[users] ([id], [username], [email], [password], [role_id], [createdat])\n',
                'VALUES (?, ?, ?, ?, ?, ?);',
              ].join(' '));
            } else if (postgresClients.includes(dbClient) || dbClient === 'sqlite') {
              expect(insertQuery).to.eql([
                'INSERT INTO "public"."users" ("id", "username", "email", "password", "role_id", "createdat")\n',
                'VALUES (?, ?, ?, ?, ?, ?);',
              ].join(' '));
            }
          });
        });

        describe('.getTableUpdateScript', () => {
          it('should return UPDATE table script', async () => {
            const updateQuery = await dbConn.getTableUpdateScript('users');
            if (dbClient === 'mysql' || dbClient === 'mariadb') {
              expect(updateQuery).to.eql([
                'UPDATE `users`\n',
                'SET `id`=?, `username`=?, `email`=?, `password`=?, `role_id`=?, `createdat`=?\n',
                'WHERE <condition>;',
              ].join(' '));
            } else if (dbClient === 'sqlserver') {
              expect(updateQuery).to.eql([
                'UPDATE [users]\n',
                'SET [id]=?, [username]=?, [email]=?, [password]=?, [role_id]=?, [createdat]=?\n',
                'WHERE <condition>;',
              ].join(' '));
            } else if (postgresClients.includes(dbClient) || dbClient === 'sqlite') {
              expect(updateQuery).to.eql([
                'UPDATE "users"\n',
                'SET "id"=?, "username"=?, "email"=?, "password"=?, "role_id"=?, "createdat"=?\n',
                'WHERE <condition>;',
              ].join(' '));
            } else if (dbClient === 'cassandra') {
              expect(updateQuery).to.eql([
                'UPDATE "users"\n',
                'SET "id"=?, "createdat"=?, "email"=?, "password"=?, "role_id"=?, "username"=?\n',
                'WHERE <condition>;',
              ].join(' '));
            } else {
              throw new Error('Invalid db client');
            }
          });

          it('should return UPDATE table script with schema if defined', async () => {
            const updateQuery = await dbConn.getTableUpdateScript('users', 'public');
            if (dbClient === 'sqlserver') {
              expect(updateQuery).to.eql([
                'UPDATE [public].[users]\n',
                'SET [id]=?, [username]=?, [email]=?, [password]=?, [role_id]=?, [createdat]=?\n',
                'WHERE <condition>;',
              ].join(' '));
            } else if (postgresClients.includes(dbClient) || dbClient === 'sqlite') {
              expect(updateQuery).to.eql([
                'UPDATE "public"."users"\n',
                'SET "id"=?, "username"=?, "email"=?, "password"=?, "role_id"=?, "createdat"=?\n',
                'WHERE <condition>;',
              ].join(' '));
            }
          });
        });

        describe('.getTableDeleteScript', () => {
          it('should return table DELETE script', async () => {
            const deleteQuery = await dbConn.getTableDeleteScript('roles');
            if (dbClient === 'mysql' || dbClient === 'mariadb') {
              expect(deleteQuery).to.contain('DELETE FROM `roles` WHERE <condition>;');
            } else if (dbClient === 'sqlserver') {
              expect(deleteQuery).to.contain('DELETE FROM [roles] WHERE <condition>;');
            } else if (postgresClients.includes(dbClient) || dbClient === 'sqlite') {
              expect(deleteQuery).to.contain('DELETE FROM "roles" WHERE <condition>;');
            } else if (dbClient === 'cassandra') {
              expect(deleteQuery).to.contain('DELETE FROM "roles" WHERE <condition>;');
            } else {
              throw new Error('Invalid db client');
            }
          });

          it('should return table DELETE script with schema if defined', async () => {
            const deleteQuery = await dbConn.getTableDeleteScript('roles', 'public');
            if (dbClient === 'sqlserver') {
              expect(deleteQuery).to.contain('DELETE FROM [public].[roles] WHERE <condition>;');
            } else if (postgresClients.includes(dbClient)) {
              expect(deleteQuery).to.contain('DELETE FROM "public"."roles" WHERE <condition>;');
            }
          });
        });

        describe('.getViewCreateScript', () => {
          it('should return CREATE VIEW script', async () => {
            const [createScript] = await dbConn.getViewCreateScript('email_view');
            if (mysqlClients.includes(dbClient)) {
              expect(createScript).to.contain([
                'VIEW `email_view`',
                'AS select `users`.`email` AS `email`,`users`.`password` AS `password`',
                'from `users`',
              ].join(' '));
            } else if (dbClient === 'postgresql') {
              expect(createScript).to.eql([
                'CREATE OR REPLACE VIEW "public".email_view AS',
                ' SELECT users.email,',
                '    users.password',
                '   FROM users;',
              ].join('\n'));
            } else if (dbClient === 'redshift') {
              expect(createScript).to.eql([
                'CREATE OR REPLACE VIEW "public".email_view AS',
                ' SELECT users.email, users."password"',
                '   FROM users;',
              ].join('\n'));
            } else if (dbClient === 'sqlserver') {
              expect(createScript).to.eql([
                '\nCREATE VIEW dbo.email_view AS',
                'SELECT dbo.users.email, dbo.users.password',
                'FROM dbo.users;\n',
              ].join('\n'));
            } else if (dbClient === 'sqlite') {
              expect(createScript).to.eql([
                'CREATE VIEW email_view AS',
                '  SELECT users.email, users.password',
                '  FROM users',
              ].join('\n'));
            } else if (dbClient === 'cassandra') {
              expect(createScript).to.eql(undefined);
            } else {
              throw new Error('Invalid db client');
            }
          });
        });

        describe('.getRoutineCreateScript', () => {
          it('should return CREATE PROCEDURE/FUNCTION script', async () => {
            const [createScript] = await dbConn.getRoutineCreateScript('users_count', 'Procedure');
            if (mysqlClients.includes(dbClient)) {
              expect(createScript).to.contain('CREATE DEFINER=');
              expect(createScript).to.contain([
                'PROCEDURE `users_count`()',
                'BEGIN',
                '  SELECT COUNT(*) FROM users;',
                'END',
              ].join('\n'));
            } else if (dbClient === 'postgresql') {
              expect(createScript).to.eql([
                'CREATE OR REPLACE FUNCTION public.users_count()',
                ' RETURNS bigint',
                ' LANGUAGE sql',
                'AS $function$',
                '  SELECT COUNT(*) FROM users AS total;',
                '$function$\n',
              ].join('\n'));
            } else if (dbClient === 'redshift') {
              expect(createScript).to.eql([
                'CREATE OR REPLACE FUNCTION public.users_count()',
                '  RETURNS bigint AS $$',
                '  SELECT COUNT(*) FROM users AS total;',
                '$$ LANGUAGE sql VOLATILE',
              ].join('\n'));
            } else if (dbClient === 'sqlserver') {
              expect(createScript).to.contain('CREATE PROCEDURE dbo.users_count');
              expect(createScript).to.contain('@Count int OUTPUT');
              expect(createScript).to.contain('SELECT @Count = COUNT(*) FROM dbo.users');
            } else if (dbClient === 'cassandra' || dbClient === 'sqlite') {
              expect(createScript).to.eql(undefined);
            } else {
              throw new Error('Invalid db client');
            }
          });
        });

        describe('.getQuerySelectTop', () => {
          let stubObj;

          beforeEach(() => {
            stubObj = stub(srcConfig, 'get');
          });

          afterEach(() => {
            stubObj.restore();
            clearLimitSelect();
          });

          it('should return select with default limit', async () => {
            stubObj.returns({});
            const sql = await dbConn.getQuerySelectTop('test_table');
            if (mysqlClients.includes(dbClient)) {
              expect(sql).to.eql('SELECT * FROM `test_table` LIMIT 1000');
            } else if (postgresClients.includes(dbClient)) {
              expect(sql).to.eql('SELECT * FROM "public"."test_table" LIMIT 1000');
            } else if (dbClient === 'sqlite' || dbClient === 'cassandra') {
              expect(sql).to.eql('SELECT * FROM "test_table" LIMIT 1000');
            } else if (dbClient === 'sqlserver') {
              expect(sql).to.eql('SELECT TOP 1000 * FROM [test_table]');
            } else {
              throw new Error('Invalid db client');
            }
          });

          it('should return select with limit from config', async () => {
            stubObj.returns({
              limitQueryDefaultSelectTop: 125,
            });
            const sql = await dbConn.getQuerySelectTop('test_table');
            if (mysqlClients.includes(dbClient)) {
              expect(sql).to.eql('SELECT * FROM `test_table` LIMIT 125');
            } else if (postgresClients.includes(dbClient)) {
              expect(sql).to.eql('SELECT * FROM "public"."test_table" LIMIT 125');
            } else if (dbClient === 'sqlite' || dbClient === 'cassandra') {
              expect(sql).to.eql('SELECT * FROM "test_table" LIMIT 125');
            } else if (dbClient === 'sqlserver') {
              expect(sql).to.eql('SELECT TOP 125 * FROM [test_table]');
            } else {
              throw new Error('Invalid db client');
            }
          });

          it('should return select with limit from parameters', async () => {
            const sql = await dbConn.getQuerySelectTop('test_table', 'public', 222);
            if (mysqlClients.includes(dbClient)) {
              expect(sql).to.eql('SELECT * FROM `test_table` LIMIT 222');
            } else if (postgresClients.includes(dbClient)) {
              expect(sql).to.eql('SELECT * FROM "public"."test_table" LIMIT 222');
            } else if (dbClient === 'sqlite' || dbClient === 'cassandra') {
              expect(sql).to.eql('SELECT * FROM "test_table" LIMIT 222');
            } else if (dbClient === 'sqlserver') {
              expect(sql).to.eql('SELECT TOP 222 * FROM [test_table]');
            } else {
              throw new Error('Invalid db client');
            }
          });
        });

        if (dbClient !== 'cassandra') {
          describe('.query', function () { // eslint-disable-line func-names
            this.timeout(15000);

            it('should be able to cancel the current query', (done) => {
              const sleepCommands = {
                postgresql: 'SELECT pg_sleep(10);',
                redshift: '',
                mysql: 'SELECT SLEEP(10000);',
                mariadb: 'SELECT SLEEP(10000);',
                sqlserver: 'WAITFOR DELAY \'00:00:10\'; SELECT 1 AS number',
                sqlite: '',
              };

              // SQLite and Redshift both do not have a way to run a sleep query.
              // Instead, we have to generate a query that will select a huge
              // data source, and take longer than 5 seconds to run. For SQLite,
              // that means doing a large select on the same table multiple times.
              // For redshift, we just run a ton of queries.
              if (dbClient === 'sqlite') {
                const fromTables = [];
                for (let i = 0; i < 50; i++) {
                  fromTables.push('sqlite_master');
                }
                sleepCommands.sqlite = `SELECT last.name FROM ${fromTables.join(',')} as last`;
              } else if (dbClient === 'redshift') {
                const queries = [];
                for (let i = 0; i < 50000; i++) {
                  queries.push(`
                    SELECT col.*, tab.*
                    FROM information_schema.columns AS col
                    INNER JOIN information_schema.tables AS tab ON col.table_schema = tab.table_schema AND col.table_name = tab.table_name
                  `);
                }
                sleepCommands.redshift = queries.join(';');
              }

              const query = dbConn.query(sleepCommands[dbClient]);
              const executing = query.execute();

              // wait a 5 secs before cancel
              setTimeout(async () => {
                let error;
                try {
                  await Promise.all([
                    executing,
                    query.cancel(),
                  ]);
                } catch (err) {
                  error = err;
                }

                try {
                  expect(error).to.exist;
                  expect(error.sqlectronError).to.eql('CANCELED_BY_USER');
                  done();
                } catch (err) {
                  done(err);
                }
              }, 5000);
            });
          });

          it('should query single result from function', async () => {
            const query = dbConn.query('SELECT CURRENT_TIMESTAMP');
            const result = await query.execute();
            expect(result[0].rows[0]).to.be.not.null;
            expect(result[0].rowCount).to.eql(1);
            let expected;
            if (dbClient === 'sqlserver') {
              expected = 1;
            } else if (dbClient === 'sqlite') {
              expected = 0;
            }
            expect(result[0].affectedRows).to.eql(expected);
          });
        }

        describe('.executeQuery', () => {
          const includePk = dbClient === 'cassandra';

          beforeEach(async () => {
            await dbConn.executeQuery(`
              INSERT INTO roles (${includePk ? 'id,' : ''} name)
              VALUES (${includePk ? '1,' : ''} 'developer')
            `);

            await dbConn.executeQuery(`
              INSERT INTO users (${includePk ? 'id,' : ''} username, email, password, role_id, createdat)
              VALUES (${includePk ? '1,' : ''} 'maxcnunes', 'maxcnunes@gmail.com', '123456', 1,'2016-10-25')
            `);
          });

          afterEach(async () => {
            await dbConn.truncateAllTables();
          });

          describe('SELECT', () => {
            it('should execute an empty query', async () => {
              try {
                const results = await dbConn.executeQuery('');
                expect(results).to.have.length(0);
              } catch (err) {
                if (dbClient === 'cassandra') {
                  expect(err.message).to.eql('line 0:-1 no viable alternative at input \'<EOF>\'');
                } else {
                  throw err;
                }
              }
            });

            it('should execute an query with only comments', async () => {
              try {
                const results = await dbConn.executeQuery('-- my comment');

                // MySQL treats commented query as a non select query
                if (dbClient === 'mysql' || dbClient === 'mariadb') {
                  expect(results).to.have.length(1);
                } else {
                  expect(results).to.have.length(0);
                }
              } catch (err) {
                if (dbClient === 'cassandra') {
                  if (versionCompare(dbConn.getVersion().version, '2') === 0) {
                    expect(err.message).to.eql('line 0:-1 no viable alternative at input \'<EOF>\'');
                  } else {
                    expect(err.message).to.eql('line 1:13 mismatched character \'<EOF>\' expecting set null');
                  }
                } else {
                  throw err;
                }
              }
            });

            it('should execute a single query with empty result', async () => {
              const results = await dbConn.executeQuery('select * from users where id = 0');

              expect(results).to.have.length(1);
              const [result] = results;

              // MSSQL/SQLite does not return the fields when the result is empty.
              // For those DBs that return the field names even when the result
              // is empty we should ensure all fields are included.
              if (dbClient === 'sqlserver' || dbClient === 'sqlite') {
                expect(result).to.have.property('fields').to.eql([]);
              } else {
                const field = (name) => result.fields.find((item) => item.name === name);

                expect(field('id')).to.exist;
                expect(field('username')).to.exist;
                expect(field('email')).to.exist;
                expect(field('password')).to.exist;
              }

              expect(result).to.have.property('command').to.eql('SELECT');
              expect(result).to.have.property('rows').to.eql([]);
              expect(result).to.have.deep.property('rowCount').to.eql(0);
            });

            it('should execute a single query', async () => {
              const results = await dbConn.executeQuery('select * from users');

              expect(results).to.have.length(1);
              const [result] = results;
              const field = (name) => result.fields.find((item) => item.name === name);

              expect(field('id')).to.exist;
              expect(field('username')).to.exist;
              expect(field('email')).to.exist;
              expect(field('password')).to.exist;
              expect(field('role_id')).to.exist;
              expect(field('createdat')).to.exist;

              expect(result).to.have.nested.property('rows[0].id').to.eql(1);
              expect(result).to.have.nested.property('rows[0].username').to.eql('maxcnunes');
              expect(result).to.have.nested.property('rows[0].password').to.eql('123456');
              expect(result).to.have.nested.property('rows[0].email').to.eql('maxcnunes@gmail.com');
              expect(result).to.have.nested.property('rows[0].createdat');

              expect(result).to.have.property('command').to.eql('SELECT');
              expect(result).to.have.deep.property('rowCount').to.eql(1);
            });

            if (postgresClients.includes(dbClient) || mysqlClients.includes(dbClient)) {
              it('should not cast DATE types to native JS Date objects', async () => {
                const results = await dbConn.executeQuery('select createdat from users');

                expect(results).to.have.length(1);
                const [result] = results;

                expect(result).to.have.nested.property('fields[0].name').to.eql('createdat');
                expect(result).to.have.nested.property('rows[0].createdat').to.match(/^2016-10-25/);
              });
            }

            it('should execute multiple queries', async () => {
              try {
                const results = await dbConn.executeQuery(`
                  select * from users;
                  select * from roles;
                `);

                expect(results).to.have.length(2);
                const [firstResult, secondResult] = results;

                expect(firstResult).to.have.nested.property('fields[0].name').to.eql('id');
                expect(firstResult).to.have.nested.property('fields[1].name').to.eql('username');
                expect(firstResult).to.have.nested.property('fields[2].name').to.eql('email');
                expect(firstResult).to.have.nested.property('fields[3].name').to.eql('password');

                expect(firstResult).to.have.nested.property('rows[0].id').to.eql(1);
                expect(firstResult).to.have.nested.property('rows[0].username').to.eql('maxcnunes');
                expect(firstResult).to.have.nested.property('rows[0].password').to.eql('123456');
                expect(firstResult).to.have.nested.property('rows[0].email').to.eql('maxcnunes@gmail.com');

                expect(firstResult).to.have.property('command').to.eql('SELECT');
                expect(firstResult).to.have.deep.property('rowCount').to.eql(1);

                expect(secondResult).to.have.nested.property('fields[0].name').to.eql('id');
                expect(secondResult).to.have.nested.property('fields[1].name').to.eql('name');

                expect(secondResult).to.have.nested.property('rows[0].id').to.eql(1);
                expect(secondResult).to.have.nested.property('rows[0].name').to.eql('developer');

                expect(secondResult).to.have.property('command').to.eql('SELECT');
                expect(secondResult).to.have.deep.property('rowCount').to.eql(1);
              } catch (err) {
                if (dbClient === 'cassandra') {
                  if (versionCompare(dbConn.getVersion().version, '3.10') >= 0) {
                    expect(err.message).to.match(/mismatched input 'select' expecting EOF/);
                  } else {
                    expect(err.message).to.match(/missing EOF at 'select'/);
                  }
                } else {
                  throw err;
                }
              }
            });
          });

          describe('INSERT', () => {
            it('should execute a single query', async () => {
              const results = await dbConn.executeQuery(`
                insert into users (${includePk ? 'id,' : ''} username, email, password)
                values (${includePk ? '1,' : ''} 'user', 'user@hotmail.com', '123456')
              `);

              expect(results).to.have.length(1);
              const [result] = results;

              expect(result).to.have.property('command').to.eql('INSERT');
              expect(result).to.have.property('rows').to.eql([]);
              expect(result).to.have.property('fields').to.eql([]);

              // Cassandra does not return affectedRows
              if (dbClient === 'cassandra') {
                expect(result).to.have.property('affectedRows').to.eql(undefined);
              } else {
                expect(result).to.have.property('affectedRows').to.eql(1);
              }

              // MSSQL does not return row count
              // so this value is based in the number of rows
              if (dbClient === 'sqlserver') {
                expect(result).to.have.property('rowCount').to.eql(0);
              } else {
                expect(result).to.have.property('rowCount').to.eql(undefined);
              }
            });

            it('should execute multiple queries', async () => {
              try {
                const results = await dbConn.executeQuery(`
                  insert into users (username, email, password)
                  values ('user', 'user@hotmail.com', '123456');

                  insert into roles (name)
                  values ('manager');
                `);

                // MSSQL treats multiple non select queries as a single query result
                if (dbClient === 'sqlserver') {
                  expect(results).to.have.length(1);
                  const [result] = results;

                  expect(result).to.have.property('command').to.eql('INSERT');
                  expect(result).to.have.property('rows').to.eql([]);
                  expect(result).to.have.property('fields').to.eql([]);
                  expect(result).to.have.property('rowCount').to.eql(0);
                  expect(result).to.have.property('affectedRows').to.eql(2);
                } else {
                  expect(results).to.have.length(2);
                  const [firstResult, secondResult] = results;

                  expect(firstResult).to.have.property('command').to.eql('INSERT');
                  expect(firstResult).to.have.property('rows').to.eql([]);
                  expect(firstResult).to.have.property('fields').to.eql([]);
                  expect(firstResult).to.have.property('rowCount').to.eql(undefined);
                  expect(firstResult).to.have.property('affectedRows').to.eql(1);

                  expect(secondResult).to.have.property('command').to.eql('INSERT');
                  expect(secondResult).to.have.property('rows').to.eql([]);
                  expect(secondResult).to.have.property('fields').to.eql([]);
                  expect(secondResult).to.have.property('rowCount').to.eql(undefined);
                  expect(secondResult).to.have.property('affectedRows').to.eql(1);
                }
              } catch (err) {
                if (dbClient === 'cassandra') {
                  if (versionCompare(dbConn.getVersion().version, '3.10') >= 0) {
                    expect(err.message).to.match(/mismatched input 'insert' expecting EOF/);
                  } else {
                    expect(err.message).to.match(/missing EOF at 'insert'/);
                  }
                } else {
                  throw err;
                }
              }
            });
          });

          describe('DELETE', () => {
            it('should execute a single query', async () => {
              const results = await dbConn.executeQuery(`
                delete from users where id = 1
              `);

              expect(results).to.have.length(1);
              const [result] = results;

              expect(result).to.have.property('command').to.eql('DELETE');
              expect(result).to.have.property('rows').to.eql([]);
              expect(result).to.have.property('fields').to.eql([]);

              // Cassandra does not return affectedRows
              if (dbClient === 'cassandra') {
                expect(result).to.have.property('affectedRows').to.eql(undefined);
              } else {
                expect(result).to.have.property('affectedRows').to.eql(1);
              }

              // MSSQL does not return row count
              // so these value is based in the number of rows
              if (dbClient === 'sqlserver') {
                expect(result).to.have.property('rowCount').to.eql(0);
              } else {
                expect(result).to.have.property('rowCount').to.eql(undefined);
              }
            });

            it('should execute multiple queries', async () => {
              try {
                const results = await dbConn.executeQuery(`
                  delete from users where username = 'maxcnunes';
                  delete from roles where name = 'developer';
                `);

                // MSSQL treats multiple non select queries as a single query result
                if (dbClient === 'sqlserver') {
                  expect(results).to.have.length(1);
                  const [result] = results;

                  expect(result).to.have.property('command').to.eql('DELETE');
                  expect(result).to.have.property('rows').to.eql([]);
                  expect(result).to.have.property('fields').to.eql([]);
                  expect(result).to.have.property('rowCount').to.eql(0);
                  expect(result).to.have.property('affectedRows').to.eql(2);
                } else {
                  expect(results).to.have.length(2);
                  const [firstResult, secondResult] = results;

                  expect(firstResult).to.have.property('command').to.eql('DELETE');
                  expect(firstResult).to.have.property('rows').to.eql([]);
                  expect(firstResult).to.have.property('fields').to.eql([]);
                  expect(firstResult).to.have.property('rowCount').to.eql(undefined);
                  expect(firstResult).to.have.property('affectedRows').to.eql(1);

                  expect(secondResult).to.have.property('command').to.eql('DELETE');
                  expect(secondResult).to.have.property('rows').to.eql([]);
                  expect(secondResult).to.have.property('fields').to.eql([]);
                  expect(secondResult).to.have.property('rowCount').to.eql(undefined);
                  expect(secondResult).to.have.property('affectedRows').to.eql(1);
                }
              } catch (err) {
                if (dbClient === 'cassandra') {
                  if (versionCompare(dbConn.getVersion().version, '3.10') >= 0) {
                    expect(err.message).to.match(/mismatched input 'delete' expecting EOF/);
                  } else {
                    expect(err.message).to.match(/missing EOF at 'delete'/);
                  }
                } else {
                  throw err;
                }
              }
            });
          });

          describe('UPDATE', () => {
            it('should execute a single query', async () => {
              const results = await dbConn.executeQuery(`
                update users set username = 'max' where id = 1
              `);

              expect(results).to.have.length(1);
              const [result] = results;

              expect(result).to.have.property('command').to.eql('UPDATE');
              expect(result).to.have.property('rows').to.eql([]);
              expect(result).to.have.property('fields').to.eql([]);

              // Cassandra does not return affectedRows
              if (dbClient === 'cassandra') {
                expect(result).to.have.property('affectedRows').to.eql(undefined);
              } else {
                expect(result).to.have.property('affectedRows').to.eql(1);
              }

              // MSSQL does not return row count
              // so these value is based in the number of rows
              if (dbClient === 'sqlserver') {
                expect(result).to.have.property('rowCount').to.eql(0);
              } else {
                expect(result).to.have.property('rowCount').to.eql(undefined);
              }
            });

            it('should execute multiple queries', async () => {
              try {
                const results = await dbConn.executeQuery(`
                  update users set username = 'max' where username = 'maxcnunes';
                  update roles set name = 'dev' where name = 'developer';
                `);

                // MSSQL treats multiple non select queries as a single query result
                if (dbClient === 'sqlserver') {
                  expect(results).to.have.length(1);
                  const [result] = results;

                  expect(result).to.have.property('command').to.eql('UPDATE');
                  expect(result).to.have.property('rows').to.eql([]);
                  expect(result).to.have.property('fields').to.eql([]);
                  expect(result).to.have.property('rowCount').to.eql(0);
                  expect(result).to.have.property('affectedRows').to.eql(2);
                } else {
                  expect(results).to.have.length(2);
                  const [firstResult, secondResult] = results;

                  expect(firstResult).to.have.property('command').to.eql('UPDATE');
                  expect(firstResult).to.have.property('rows').to.eql([]);
                  expect(firstResult).to.have.property('fields').to.eql([]);
                  expect(firstResult).to.have.property('rowCount').to.eql(undefined);
                  expect(firstResult).to.have.property('affectedRows').to.eql(1);

                  expect(secondResult).to.have.property('command').to.eql('UPDATE');
                  expect(secondResult).to.have.property('rows').to.eql([]);
                  expect(secondResult).to.have.property('fields').to.eql([]);
                  expect(secondResult).to.have.property('rowCount').to.eql(undefined);
                  expect(secondResult).to.have.property('affectedRows').to.eql(1);
                }
              } catch (err) {
                if (dbClient === 'cassandra') {
                  if (versionCompare(dbConn.getVersion().version, '3.10') >= 0) {
                    expect(err.message).to.match(/mismatched input 'update' expecting EOF/);
                  } else {
                    expect(err.message).to.match(/missing EOF at 'update'/);
                  }
                } else {
                  throw err;
                }
              }
            });
          });

          if (dbClient !== 'cassandra' && dbClient !== 'sqlite') {
            describe('CREATE', () => {
              describe('DATABASE', () => {
                beforeEach(async () => {
                  try {
                    await dbConn.executeQuery('drop database db_test_create_database');
                  } catch (err) {
                    // just ignore
                  }
                });

                it('should execute a single query', async () => {
                  const results = await dbConn.executeQuery('create database db_test_create_database');

                  // MSSQL does not return any information about CREATE queries
                  if (dbClient === 'sqlserver') {
                    expect(results).to.have.length(0);
                    return;
                  }

                  expect(results).to.have.length(1);
                  const [result] = results;

                  expect(result).to.have.property('command').to.eql('CREATE_DATABASE');
                  expect(result).to.have.property('rows').to.eql([]);
                  expect(result).to.have.property('fields').to.eql([]);
                  // seems each DB client returns a different value for CREATE
                  expect(result).to.have.property('affectedRows').to.oneOf([0, 1, undefined]);
                  expect(result).to.have.property('rowCount').to.eql(undefined);
                });
              });
            });
          }

          if (dbClient !== 'cassandra' && dbClient !== 'sqlite') {
            describe('DROP', () => {
              describe('DATABASE', () => {
                beforeEach(async () => {
                  try {
                    await dbConn.executeQuery('create database db_test_create_database');
                  } catch (err) {
                    // just ignore
                  }
                });

                it('should execute a single query', async () => {
                  const results = await dbConn.executeQuery('drop database db_test_create_database');

                  // MSSQL does not return any information about DROP queries
                  if (dbClient === 'sqlserver') {
                    expect(results).to.have.length(0);
                    return;
                  }

                  expect(results).to.have.length(1);
                  const [result] = results;

                  expect(result).to.have.property('command').to.eql('DROP_DATABASE');
                  expect(result).to.have.property('rows').to.eql([]);
                  expect(result).to.have.property('fields').to.eql([]);
                  // seems each DB client returns a different value for DROP
                  expect(result).to.have.property('affectedRows').to.oneOf([0, 1, undefined]);
                  expect(result).to.have.property('rowCount').to.eql(undefined);
                });
              });
            });
          }

          if (postgresClients.includes(dbClient)) {
            describe('EXPLAIN', () => {
              it('should execute a single query', async () => {
                const results = await dbConn.executeQuery('explain select * from users');

                expect(results).to.have.length(1);
                const [result] = results;

                expect(result).to.have.property('command').to.eql('EXPLAIN');
                expect(result).to.have.property('rows').to.have.length.above(0);
                expect(result).to.have.deep.property('fields').to.have.length(1);
                expect(result).to.have.nested.property('fields[0].name').to.eql('QUERY PLAN');
                expect(result).to.have.property('affectedRows').to.eql(undefined);
                expect(result).to.have.property('rowCount').to.eql(undefined);
              });
            });
          }
        });
      });
    });
  });
});
