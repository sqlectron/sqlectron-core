/**
 * unified-dataloader-core
 * Copyright (C) 2018 Armarti Industries
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import { createConnection } from './client';
import { CLIENTS } from './clients';


export function createServer(serverConfig) {
  if (!serverConfig) {
    throw new Error('Missing server configuration');
  }

  if (!CLIENTS.some((cli) => cli.key === serverConfig.client)) {
    throw new Error('Invalid SQL client');
  }

  const server = {
    /**
     * All connected dbs
     */
    db: {},

    config: {
      ...serverConfig,
      host: serverConfig.host || serverConfig.socketPath,
    },
  };

  /**
  * Server public API
  */
  return {
    db(dbName) {
      return server.db[dbName];
    },

    end() {
      // disconnect from all DBs
      Object.keys(server.db).forEach((key) => server.db[key].disconnect());

      // close SSH tunnel
      if (server.sshTunnel) {
        server.sshTunnel.close();
        server.sshTunnel = null;
      }
    },

    createConnection(dbName, cryptoSecret) {
      if (server.db[dbName]) {
        return server.db[dbName];
      }

      const database = {
        database: dbName,
        connection: null,
        connecting: false,
      };

      server.db[dbName] = createConnection(server, database, cryptoSecret);

      return server.db[dbName];
    },
  };
}
