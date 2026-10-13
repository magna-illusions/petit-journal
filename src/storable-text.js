// What Postgres can store as text, checked before anything is written. Its
// text and jsonb columns refuse a string with \u0000, and pg writes half a
// surrogate pair as U+FFFD, so a value would not come back as written; the
// mock keeps values in memory and would take either. So both stores refuse
// such a string, in a value or a key, and the message names where it is.
const problemIn = (text) => (text.includes('\u0000') ? '\\u0000' : !text.isWellFormed() ? 'a lone surrogate' : null)

function checkStorableText(value, label) {
  if (typeof value === 'string') {
    const problem = problemIn(value)
    if (problem) throw new TypeError(`${label} contains ${problem}, which Postgres cannot store`)
  } else if (Array.isArray(value)) {
    value.forEach((item, i) => checkStorableText(item, `${label}[${i}]`))
  } else if (typeof value === 'object' && value !== null) {
    for (const [key, item] of Object.entries(value)) {
      const problem = problemIn(key)
      if (problem) throw new TypeError(`${label} has a key that contains ${problem}, which Postgres cannot store`)
      checkStorableText(item, `${label}.${key}`)
    }
  }
}

export { checkStorableText }
