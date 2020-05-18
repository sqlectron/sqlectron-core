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
function encryptSecrects(server, cryptoSecret, oldServer) {
  const updatedServer = { ...server };

  if (server.password) {
    if (oldServer && oldServer.password && typeof oldServer.password === 'string' && oldServer.encrypted) {
      if (server.password === oldServer.password) {
        updatedServer.password = crypto.unsafeDecrypt(oldServer.password, cryptoSecret);
      }
    }

    if (typeof updatedServer.password === 'string') {
      updatedServer.password = crypto.encrypt(updatedServer.password, cryptoSecret);
    }
  }

  if (server.ssh && server.ssh.password) {
    if (oldServer && oldServer.ssh && oldServer.ssh.password && typeof oldServer.ssh.password === 'string' && oldServer.encrypted) {
      if (server.password === oldServer.password) {
        updatedServer.password = crypto.unsafeDecrypt(oldServer.password, cryptoSecret);
      }
    }

    if (typeof updatedServer.ssh.password === 'string') {
      updatedServer.ssh.password = crypto.encrypt(updatedServer.ssh.password, cryptoSecret);
    }
  }

  updatedServer.encrypted = true;
  return updatedServer;
}

// decrypt secret fields
export function decryptSecrects(server, cryptoSecret) {
  const updatedServer = { ...server };
  if (!server.encrypted) {
    return server;
  }

  if (server.password && typeof server.password === 'string') {
    updatedServer.password = crypto.unsafeDecrypt(server.password, cryptoSecret);
  } else if (server.password) {
    updatedServer.password = crypto.decrypt(server.password, cryptoSecret);
  }

  if (server.ssh && server.ssh.password && typeof server.ssh.password === 'string') {
    updatedServer.ssh.password = crypto.unsafeDecrypt(server.ssh.password, cryptoSecret);
  } else if (server.ssh && server.ssh.password) {
    updatedServer.ssh.password = crypto.decrypt(server.ssh.password, cryptoSecret);
  }

  updatedServer.encrypted = false;
  return updatedServer;
}
