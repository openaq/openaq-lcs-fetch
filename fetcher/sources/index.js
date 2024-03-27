const fs = require('fs');
/**
 * A collection of source configurations
 @exports Source[]
 */

/**
 * @typedef {Object} Source
 *
 * @property {'v1'} schema
 * @property {string} provider
 * @property {('minute'|'hour'|'day')} frequency
 * @property {object} meta
 */

/** @type {Source[]} */
module.exports = fs.readdirSync(__dirname)
    .filter((f) => f.endsWith('.json'))
    .map((f) => require(`./${f}`))
		.filter((f) => f.active);
