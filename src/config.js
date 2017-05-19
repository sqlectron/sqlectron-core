import uuid from 'uuid';
import * as utils from './utils';

const EMPTY_CONFIG = { servers: [] };

function sanitizeServers(data) {
  return data.servers.map((server) => {
    const srv = { ...server };
    // ensure all server has an unique id
    if (!srv.id) { srv.id = uuid.v4(); }

    // ensure all servers has the new fileld SSL
    if (srv.ssl === undefined) { srv.ssl = false; }

    // store password as a hash instead of plain text
    if (!srv.hashedPwd) {
      srv.password = utils.encryptString(srv.password);
      srv.hashedPwd = true;
    }

    return srv;
  });
}

/**
 * Prepare the configuration file sanitizing and validating all fields availbale
 */
export async function prepare() {
  const filename = utils.getConfigPath();
  const fileExistsResult = await utils.fileExists(filename);
  if (!fileExistsResult) {
    await utils.writeJSONFile(filename, EMPTY_CONFIG);
  }

  const result = await utils.readJSONFile(filename);

  result.servers = sanitizeServers(result);

  await utils.writeJSONFile(filename, result);

  // TODO: Validate whole configuration file
  // if (!configValidate(result)) {
  //   throw new Error('Invalid ~/.sqlectron.json file format');
  // }
}

export function prepareSync() {
  const filename = utils.getConfigPath();
  const fileExistsResult = utils.fileExistsSync(filename);
  if (!fileExistsResult) {
    utils.writeJSONFileSync(filename, EMPTY_CONFIG);
  }

  const result = utils.readJSONFileSync(filename);

  result.servers = sanitizeServers(result);

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
  return utils.readJSONFile(filename)
    .then((data) => {
      const configData = { ...data };
      configData.servers = configData.servers.map((server) => {
        const srv = { ...server };
        if (srv.hashedPwd) { srv.password = utils.decryptString(srv.password); }
        return srv;
      });
      return configData;
    });
}

export function getSync() {
  const filename = utils.getConfigPath();
  const data = utils.readJSONFileSync(filename);
  data.servers = data.servers.map((server) => {
    const srv = { ...server };
    if (srv.hashedPwd) { srv.password = utils.decryptString(srv.password); }
    return srv;
  });
  return data;
}


export function save(data) {
  const filename = utils.getConfigPath();
  const configData = { ...data };
  configData.servers = configData.servers.map((server) => {
    const srv = { ...server };
    // encrypt password
    srv.password = utils.encryptString(srv.password);
    srv.hashedPwd = true;
    return srv;
  });
  return utils.writeJSONFile(filename, configData);
}
