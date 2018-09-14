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
import uuid from 'uuid';
import { validate, validateUniqueId } from './validators/server';
import * as config from './config';
import * as crypto from './crypto';


export async function getAll() {
  const { servers } = await config.get();
  return servers;
}


export async function add(server, cryptoSecret) {
  let srv = { ...server };
  await validate(srv);

  const data = await config.get();
  const newId = uuid.v4();
  validateUniqueId(data.servers, newId);

  srv = encryptSecrects(srv, cryptoSecret);

  srv.id = newId;
  data.servers.push(srv);
  await config.save(data);

  return srv;
}


export async function update(server, cryptoSecret) {
  let srv = { ...server };
  await validate(srv);

  const data = await config.get();
  validateUniqueId(data.servers, srv.id);

  const index = data.servers.findIndex((item) => item.id === srv.id);
  srv = encryptSecrects(srv, cryptoSecret, data.servers[index]);

  data.servers = [
    ...data.servers.slice(0, index),
    srv,
    ...data.servers.slice(index + 1),
  ];

  await config.save(data);

  return server;
}


export function addOrUpdate(server, cryptoSecret) {
  const hasId = !!(server.id && String(server.id).length);
  // TODO: Add validation to check if the current id is a valid uuid
  return hasId ? update(server, cryptoSecret) : add(server, cryptoSecret);
}


export async function removeById(id) {
  const data = await config.get();

  const index = data.servers.findIndex((srv) => srv.id === id);
  data.servers = [
    ...data.servers.slice(0, index),
    ...data.servers.slice(index + 1),
  ];

  await config.save(data);
}

// ensure all secret fields are encrypted
function encryptSecrects(server, cryptoSecret, oldSever) {
  const updatedServer = { ...server };

  /* eslint no-param-reassign:0 */
  if (server.password) {
    const isPassDiff = (oldSever && server.password !== oldSever.password);

    if (!oldSever || isPassDiff) {
      updatedServer.password = crypto.encrypt(server.password, cryptoSecret);
    }
  }

  if (server.ssh && server.ssh.password) {
    const isPassDiff = (oldSever && server.ssh.password !== oldSever.ssh.password);

    if (!oldSever || isPassDiff) {
      updatedServer.ssh.password = crypto.encrypt(server.ssh.password, cryptoSecret);
    }
  }

  updatedServer.encrypted = true;
  return updatedServer;
}

// decrypt secret fields
export function decryptSecrects(server, cryptoSecret) {
  const updatedServer = { ...server };
  /* eslint no-param-reassign:0 */
  if (!server.encrypted) {
    return;
  }

  if (server.password) {
    updatedServer.password = crypto.decrypt(server.password, cryptoSecret);
  }

  if (server.ssh && server.ssh.password) {
    updatedServer.ssh.password = crypto.decrypt(server.ssh.password, cryptoSecret);
  }

  updatedServer.encrypted = false;
  return updatedServer;
}
