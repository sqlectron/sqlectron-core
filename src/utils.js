import fs from 'fs';
import { homedir } from 'os';
import path from 'path';
import mkdirp from 'mkdirp';
import pf from 'portfinder';
import envPaths from 'env-paths';

let configPath = '';

export function getConfigPath() {
  if (configPath) {
    return configPath;
  }

  const configName = 'sqlectron.json';
  const oldConfigPath = path.join(homedir(), `.${configName}`);

  if (fileExistsSync(oldConfigPath)) {
    configPath = oldConfigPath;
  } else {
    const newConfigDir = envPaths('Sqlectron', { suffix: '' }).config;
    configPath = path.join(newConfigDir, configName);
  }

  return configPath;
}


export function fileExists(filename) {
  return new Promise((resolve) => {
    fs.stat(filename, (err, stats) => {
      if (err) return resolve(false);
      resolve(stats.isFile());
    });
  });
}


export function fileExistsSync(filename) {
  try {
    return fs.statSync(filename).isFile();
  } catch (e) {
    return false;
  }
}


export function writeFile(filename, data) {
  return new Promise((resolve, reject) => {
    fs.writeFile(filename, data, (err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}


export function writeJSONFile(filename, data) {
  return writeFile(filename, JSON.stringify(data, null, 2));
}


export function writeJSONFileSync(filename, data) {
  return fs.writeFileSync(filename, JSON.stringify(data, null, 2));
}


export function readFile(filename) {
  const filePath = resolveHomePathToAbsolute(filename);
  return new Promise((resolve, reject) => {
    fs.readFile(path.resolve(filePath), { encoding: 'utf-8' }, (err, data) => {
      if (err) return reject(err);
      resolve(data);
    });
  });
}


export function readJSONFile(filename) {
  return readFile(filename).then((data) => JSON.parse(data));
}


export function readJSONFileSync(filename) {
  const filePath = resolveHomePathToAbsolute(filename);
  const data = fs.readFileSync(path.resolve(filePath), { enconding: 'utf-8' });
  return JSON.parse(data);
}

export function createParentDirectory(filename) {
  return new Promise((resolve, reject) =>
    (mkdirp(path.dirname(filename), (err) => (err ? reject(err) : resolve()))),
  );
}

export function createParentDirectorySync(filename) {
  mkdirp.sync(path.dirname(filename));
}


export function resolveHomePathToAbsolute(filename) {
  if (!/^~\//.test(filename)) {
    return filename;
  }

  return path.join(homedir(), filename.substring(2));
}


export function getPort() {
  return new Promise((resolve, reject) => {
    pf.getPort({ host: 'localhost' }, (err, port) => {
      if (err) return reject(err);
      resolve(port);
    });
  });
}

export function createCancelablePromise(error, timeIdle = 100) {
  let canceled = false;
  let discarded = false;

  const wait = (time) => new Promise((resolve) => setTimeout(resolve, time));

  return {
    async wait() {
      while (!canceled && !discarded) {
        // eslint-disable-next-line
        await wait(timeIdle);
      }

      if (canceled) {
        const err = new Error(error.message || 'Promise canceled.');

        Object.getOwnPropertyNames(error)
          .forEach((key) => err[key] = error[key]); // eslint-disable-line no-return-assign

        throw err;
      }
    },
    cancel() {
      canceled = true;
    },
    discard() {
      discarded = true;
    },
  };
}

/**
 * Compares two version strings.
 *
 * For two version strings, this fucntion will return -1 if the first version is smaller
 * than the second version, 0 if they are equal, and 1 if the second version is smaller.
 * However, this function will only compare up-to the smallest part of the version string
 * defined between the two, such '8' and '8.0.2' will be considered equal.
 *
 * @param {string} a
 * @param {string} b
 * @returns {boolean}
 */
export function versionCompare(a, b) {
  const fullA = a.split('.').map((val) => parseInt(val, 10));
  const fullB = b.split('.').map((val) => parseInt(val, 10));

  for (let i = 0; i < Math.min(fullA.length, fullB.length); i++) {
    if (fullA[i] > fullB[i]) {
      return 1;
    } else if (fullA[i] < fullB[i]) {
      return -1;
    }
  }
  return 0;
}
