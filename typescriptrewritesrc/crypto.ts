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
// Reference: http://lollyrock.com/articles/nodejs-encryption
import crypto from 'crypto';

const algorithm = 'aes-256-ctr';

export function encrypt(text, secret) {
  if (!secret) {
    throw new Error('Missing crypto secret');
  }

  const cipher = crypto.createCipher(algorithm, secret);
  let crypted = cipher.update(text, 'utf8', 'hex');
  crypted += cipher.final('hex');
  return crypted;
}

export function decrypt(text, secret) {
  if (!secret) {
    throw new Error('Missing crypto secret');
  }

  const decipher = crypto.createDecipher(algorithm, secret);
  let dec = decipher.update(text, 'hex', 'utf8');
  dec += decipher.final('utf8');
  return dec;
}
