// @ts-check
// The codec the README's "Encrypting payloads" points to: `npm run typecheck`
// checks it against PayloadCodec and node's own types, and readme.test.js runs
// it. Nothing ships it. Use: openEventStore(pool, { codec: encryptingCodec(keyFor) }).

import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto'

// Binds each ciphertext to its stream and event type (none for a projection's state).
const aad = (c) => Buffer.from(JSON.stringify([c.kind, c.ownerId, c.stream, c.subjectId, c.type ?? null]))
const b64 = (bytes) => bytes.toString('base64')

/**
 * @param {(ownerId: string) => Buffer | Promise<Buffer>} keyFor a 32-byte key per owner
 * @returns {import('petit-journal').PayloadCodec}
 */
export function encryptingCodec(keyFor) {
  const key = async (/** @type {string} */ ownerId) => {
    const bytes = await keyFor(ownerId)
    if (!Buffer.isBuffer(bytes) || bytes.length !== 32) throw new TypeError('keyFor must return a 32-byte Buffer')
    return bytes
  }
  return {
    async encode(value, context) {
      const iv = randomBytes(12)
      const cipher = createCipheriv('aes-256-gcm', await key(context.ownerId), iv).setAAD(aad(context))
      const data = Buffer.concat([cipher.update(JSON.stringify(value)), cipher.final()])
      return { v: 1, iv: b64(iv), tag: b64(cipher.getAuthTag()), data: b64(data) }
    },
    async decode(stored, context) {
      const { v, iv, tag, data } = /** @type {{ v?: number, iv: string, tag: string, data: string }} */ (stored ?? {})
      if (v !== 1) throw new Error('not a value this codec sealed (v1)')
      const [ivBytes, tagBytes] = [Buffer.from(iv, 'base64'), Buffer.from(tag, 'base64')]
      if (ivBytes.length !== 12 || tagBytes.length !== 16) throw new Error('a sealed value needs a 12-byte iv and a 16-byte tag')
      const decipher = createDecipheriv('aes-256-gcm', await key(context.ownerId), ivBytes, { authTagLength: 16 })
      decipher.setAAD(aad(context)).setAuthTag(tagBytes)
      return JSON.parse(Buffer.concat([decipher.update(data, 'base64'), decipher.final()]).toString())
    },
  }
}
