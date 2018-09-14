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
import net from 'net';
import { Client } from 'ssh2';
import { getPort, readFile } from '../utils';
import createLogger from '../logger';

const logger = createLogger('db:tunnel');

export default function (serverInfo) {
  return new Promise(async (resolve, reject) => {
    logger().debug('configuring tunnel');
    const config = await configTunnel(serverInfo);

    const connections = [];

    logger().debug('creating ssh tunnel server');
    const server = net.createServer(async (conn) => {
      conn.on('error', (err) => server.emit('error', err));

      logger().debug('creating ssh tunnel client');
      const client = new Client();
      connections.push(conn);

      client.on('error', (err) => server.emit('error', err));

      client.on('ready', () => {
        logger().debug('connected ssh tunnel client');
        connections.push(client);

        logger().debug('forwarding ssh tunnel client output');
        client.forwardOut(
          config.srcHost,
          config.srcPort,
          config.dstHost,
          config.dstPort,
          (err, sshStream) => {
            if (err) {
              logger().error('error ssh connection %j', err);
              server.close();
              server.emit('error', err);
              return;
            }
            server.emit('success');
            conn.pipe(sshStream).pipe(conn);
          });
      });

      try {
        const localPort = await getPort();

        logger().debug('connecting ssh tunnel client');
        client.connect({ ...config, localPort });
      } catch (err) {
        server.emit('error', err);
      }
    });

    server.once('close', () => {
      logger().debug('close ssh tunnel server');
      connections.forEach((conn) => conn.end());
    });

    logger().debug('connecting ssh tunnel server');
    server.listen(config.localPort, config.localHost, (err) => {
      if (err) return reject(err);

      logger().debug('connected ssh tunnel server');
      resolve(server);
    });
  });
}


async function configTunnel(serverInfo) {
  const config = {
    username: serverInfo.ssh.user,
    port: serverInfo.ssh.port,
    host: serverInfo.ssh.host,
    dstPort: serverInfo.port,
    dstHost: serverInfo.host,
    sshPort: 22,
    srcPort: 0,
    srcHost: 'localhost',
    localHost: 'localhost',
    localPort: await getPort(),
  };
  if (serverInfo.ssh.password) config.password = serverInfo.ssh.password;
  if (serverInfo.ssh.passphrase) config.passphrase = serverInfo.ssh.passphrase;
  if (serverInfo.ssh.privateKey) {
    config.privateKey = await readFile(serverInfo.ssh.privateKey);
  }
  return config;
}
