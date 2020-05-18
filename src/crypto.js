// Reference: http://lollyrock.com/articles/nodejs-encryption
import crypto from 'crypto';

const algorithm = 'aes-256-cbc';

export function encrypt(plainText, secret) {
  if (!plainText) {
    throw new Error('Missing plain text');
  } else if (!secret) {
    throw new Error('Missing encrypt secret');
  }
  let key = Buffer.alloc(32);
  key = Buffer.concat([Buffer.from(secret)], key.length);
  const ivBytes = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(algorithm, key, ivBytes);
  return {
    ivText: ivBytes.toString('base64'),
    encryptedText: cipher.update(plainText, 'utf8', 'base64') + cipher.final('base64'),
  };
}

export function decrypt(encrypted, secret) {
  if (!encrypted || !encrypted.ivText || !encrypted.encryptedText) {
    throw new Error('Invalid encrypted valued');
  } else if (!secret) {
    throw new Error('Missing decrypt secret');
  }

  const iv = Buffer.from(encrypted.ivText, 'base64');
  let key = Buffer.alloc(32);
  key = Buffer.concat([Buffer.from(secret)], key.length);

  if (iv.length !== 16) {
    throw new Error('The encrypted value is not a valid format');
  }

  if (key.length !== 32) {
    throw new Error('The secret is not valid format');
  }

  const decipher = crypto.createDecipheriv(algorithm, key, iv);
  return decipher.update(encrypted.encryptedText, 'base64', 'utf8') + decipher.final('utf8');
}

/**
 * Decrypts a value using the insecure createDecipher method.
 *
 * This method should not be used and only exists to give an upgrade path
 * of existing sqlectron-core users to the much better iv functions above.
 *
 * @deprecated 8.1.0
 * @param {string} text
 * @param {string} secret
 */
export function unsafeDecrypt(text, secret) {
  if (!secret) {
    throw new Error('Missing crypto secret');
  }

  const decipher = crypto.createDecipher('aes-256-ctr', secret);
  return decipher.update(text, 'hex', 'utf8') + decipher.final('utf8');
}
