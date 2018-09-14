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
import Valida from 'valida';
import { CLIENTS } from '../db';


function serverAddressValidator(ctx) {
  const { host, port, socketPath } = ctx.obj;
  if ((!host && !port && !socketPath) || ((host || port) && socketPath)) {
    return {
      validator: 'serverAddressValidator',
      msg: 'You must use host+port or socket path',
    };
  }

  if (socketPath) { return undefined; }

  if ((host && !port) || (!host && port)) {
    return {
      validator: 'serverAddressValidator',
      msg: 'Host and port are required fields.',
    };
  }
}


function clientValidator(ctx, options, value) {
  if (typeof value === 'undefined' || value === null) { return undefined; }
  if (!~CLIENTS.some((dbClient) => dbClient.key === ctx.obj.client)) {
    return {
      validator: 'clientValidator',
      msg: 'Invalid client type',
    };
  }
}


function boolValidator(ctx, options, value) {
  if (typeof value === 'undefined' || value === null) { return undefined; }
  if (value !== true && value !== false) {
    return {
      validator: 'boolValidator',
      msg: 'Invalid boolean type.',
    };
  }
}


const SSH_SCHEMA = {
  host: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.len, min: 1 },
  ],
  port: [
    { sanitizer: Valida.Sanitizer.toInt },
    { validator: Valida.Validator.len, min: 1, max: 5 },
  ],
  user: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.required },
    { validator: Valida.Validator.len, min: 1 },
  ],
  password: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.len, min: 1 },
  ],
  privateKey: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.len, min: 1 },
  ],
  privateKeyWithPassphrase: [
    { validator: boolValidator },
  ],
};


const SERVER_SCHEMA = {
  name: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.required },
    { validator: Valida.Validator.len, min: 1 },
  ],
  client: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.required },
    { validator: clientValidator },
  ],
  ssl: [
    { validator: Valida.Validator.required },
  ],
  host: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.len, min: 1 },
    { validator: serverAddressValidator },
  ],
  port: [
    { sanitizer: Valida.Sanitizer.toInt },
    { validator: Valida.Validator.len, min: 1, max: 5 },
    { validator: serverAddressValidator },
  ],
  socketPath: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.len, min: 1 },
    { validator: serverAddressValidator },
  ],
  database: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.len, min: 1 },
  ],
  user: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.len, min: 1 },
  ],
  password: [
    { sanitizer: Valida.Sanitizer.trim },
    { validator: Valida.Validator.len, min: 1 },
  ],
  ssh: [
    { validator: Valida.Validator.schema, schema: SSH_SCHEMA },
  ],
};


/**
 * validations applied on creating/updating a server
 */
export async function validate(server) {
  const serverSchema = { ...SERVER_SCHEMA };

  const clientConfig = CLIENTS.find((dbClient) => dbClient.key === server.client);
  if (clientConfig && clientConfig.disabledFeatures) {
    clientConfig.disabledFeatures.forEach((item) => {
      const [region, field] = item.split(':');
      if (region === 'server') {
        delete serverSchema[field];
      }
    });
  }

  const validated = await Valida.process(server, serverSchema);
  if (!validated.isValid()) { throw validated.invalidError(); }
}


export function validateUniqueId(servers, serverId) {
  if (!serverId) { return; }

  const server = servers.find((srv) => srv.id === serverId);
  if (!server) { return; }
  if (serverId && server.id === serverId) { return; }

  throw new Error('Already exist another server with same id');
}
