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
import * as utils from './utils';
import * as crypto from './crypto';

const EMPTY_CONFIG = { servers: [] };

function sanitizeServer(server, cryptoSecret) {
  const srv = { ...server };

  // ensure has an unique id
  if (!srv.id) { srv.id = uuid.v4(); }

  // ensure has the new fileld SSL
  if (typeof srv.ssl === 'undefined') { srv.ssl = false; }

  // ensure all secret fields are encrypted
  if (typeof srv.encrypted === 'undefined') {
    srv.encrypted = true;

    if (srv.password) {
      srv.password = crypto.encrypt(srv.password, cryptoSecret);
    }

    if (srv.ssh && srv.ssh.password) {
      srv.ssh.password = crypto.encrypt(srv.ssh.password, cryptoSecret);
    }
  }

  return srv;
}

function sanitizeServers(data, cryptoSecret) {
  return data.servers
    .map((server) => sanitizeServer(server, cryptoSecret));
}

/**
 * Prepare the configuration file sanitizing and validating all fields availbale
 */
export async function prepare(cryptoSecret) {
  const filename = utils.getConfigPath();
  const fileExistsResult = await utils.fileExists(filename);
  if (!fileExistsResult) {
    await utils.createParentDirectory(filename);
    await utils.writeJSONFile(filename, EMPTY_CONFIG);
  }

  const result = await utils.readJSONFile(filename);

  result.servers = sanitizeServers(result, cryptoSecret);

  await utils.writeJSONFile(filename, result);

  // TODO: Validate whole configuration file
  // if (!configValidate(result)) {
  //   throw new Error('Invalid ~/.sqlectron.json file format');
  // }
}

export function prepareSync(cryptoSecret) {
  const filename = utils.getConfigPath();
  const fileExistsResult = utils.fileExistsSync(filename);
  if (!fileExistsResult) {
    utils.createParentDirectorySync(filename);
    utils.writeJSONFileSync(filename, EMPTY_CONFIG);
  }

  const result = utils.readJSONFileSync(filename);

  result.servers = sanitizeServers(result, cryptoSecret);

  utils.writeJSONFileSync(filename, result);

  // TODO: Validate whole configuration file
  // if (!configValidate(result)) {
  //   throw new Error('Invalid ~/.sqlectron.json file format');
  // }
}

export function path() {
  const filename = utils.getConfigPath();
  return utils.resolveHomePathToAbsolute(filename);
}

export function get() {
  const filename = utils.getConfigPath();
  return utils.readJSONFile(filename);
}

export function getSync() {
  const filename = utils.getConfigPath();
  return utils.readJSONFileSync(filename);
}


export function save(data) {
  const filename = utils.getConfigPath();
  return utils.writeJSONFile(filename, data);
}


export async function saveSettings(data) {
  const fullData = await get();
  const filename = utils.getConfigPath();
  const newData = { ...fullData, ...data };
  return utils.writeJSONFile(filename, newData);
}
