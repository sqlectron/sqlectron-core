import { expect } from 'chai';
import { validate, validateUniqueId } from '../../src/validators/server';

describe('validators/server', () => {
  describe('validate', () => {
    [
      {
        id: 'c94cbafa-8977-4142-9f34-c84d382d8731',
        name: 'pg-vm',
        client: 'postgresql',
        host: '10.10.10.10',
        port: 5432,
        user: 'user',
        password: 'password',
        database: 'company',
        ssl: false,
      },
      {
        id: 'c94cbafa-8977-4142-9f34-c84d382d8731',
        name: 'pg-vm',
        client: 'postgresql',
        host: '10.10.10.10',
        port: 5432,
        user: 'user',
        password: {
          ivText: 'wGf6X9T+QSygOHqtgQPlcA==',
          encryptedText: '0LySDs9WPAvwSS9Qv+W3/A==',
        },
        database: 'company',
        ssl: false,
      },
      {
        encrypted: true,
        id: '179d7c6e-2d7c-4c86-b203-d901b7dfea77',
        name: 'mysql',
        client: 'mysql',
        host: '10.10.10.15',
        port: 3306,
        database: 'authentication',
        user: 'root',
        password: 'fa1d88ee82bd4439',
        ssh: {
          host: '10.10.10.10',
          port: 22,
          user: 'core',
          password: 'fa1d88ee82bd4439',
        },
        ssl: false,
      },
      {
        id: 'f7baa5f0-42a2-4f51-b49d-26cf3bfe8221',
        name: 'pg-vm-ssh',
        client: 'postgresql',
        host: 'localhost',
        port: 5432,
        user: 'user',
        password: 'password',
        database: 'company',
        ssh: {
          host: '10.10.10.10',
          port: 22,
          privateKey: '~/.vagrant.d/insecure_private_key',
          user: 'core',
        },
        ssl: false,
      },
      {
        id: 'df8b16d3-6ad4-4582-a5fd-58d18b5b50ae',
        name: 'Name',
        client: 'mysql',
        host: 'host.com',
        port: 33,
        user: 'user',
        password: {
          ivText: 'nqvh1Gks3Yhs9OhWEmPcXg==',
          encryptedText: 'CLYS5FE/Evtx37LKJHbVJw==',
        },
        database: 'database',
        ssh: {
          host: 'hostsssss.com',
          port: 22,
          user: 'userssh',
          password: {
            ivText: 'lyDr1yAtBa1MoLsJlsCFfA==',
            encryptedText: 'Z9TrH05/Csxl9xA/FgIVXw==',
          },
        },
        encrypted: true,
        ssl: false,
      },
    ].forEach((server, idx) => {
      it(`should validate server ${idx}`, (done) => {
        validate(server).then(() => done()).catch((err) => done(err));
      });
    });

    ['mysql', 'postgresql', 'sqlserver', 'sqlite', 'cassandra'].forEach((client) => {
      it(`should validate client ${client}`, (done) => {
        const server = {
          id: 'c94cbafa-8977-4142-9f34-c84d382d8731',
          name: 'name',
          client,
          host: '10.10.10.10',
          port: 5432,
          user: 'user',
          password: 'password',
          database: 'company',
          ssl: false,
        };
        validate(server).then(() => done()).catch((err) => done(err));
      });
    });

    it('should not validate invalid client', (done) => {
      const server = {
        id: 'c94cbafa-8977-4142-9f34-c84d382d8731',
        name: 'pg-vm',
        client: 'fake-client',
        host: '10.10.10.10',
        port: 5432,
        user: 'user',
        password: 'password',
        database: 'company',
        ssl: false,
      };
      validate(server).then(() => done(new Error('should have thrown error'))).catch((err) => {
        expect(err.message).to.eql('validaInvalidError');
        done();
      });
    });
  });

  describe('validateUniqueId', () => {
    [undefined, null].forEach((serverId) => {
      it(`should throw when serverId is ${JSON.stringify(serverId)}`, () => {
        expect(() => validateUniqueId([], serverId)).to.throw('serverId should be set');
      });
    });

    it('should validate when serverId is not found', () => {
      expect(validateUniqueId([], '1c7cdae9-6065-46fa-a7d0-b89ccff78703')).to.eql(true);
    });

    it('should fail when serverId is found', () => {
      expect(validateUniqueId([{ id: '1c7cdae9-6065-46fa-a7d0-b89ccff78703' }], '1c7cdae9-6065-46fa-a7d0-b89ccff78703')).to.eql(false);
    });
  });
});
