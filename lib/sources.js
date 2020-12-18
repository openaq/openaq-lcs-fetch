'use strict';

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
module.exports = [
    {
        schema: 'v1',
        provider: 'cmu',
        frequency: 'hour',
        meta: {
            folderId: '1Mp_a-OyGGlk5tGkezYK41iZ2qybnrPzp'
        }
    },
    {
        schema: 'v1',
        provider: 'habitatmap',
        frequency: 'minute',
        meta: {
            url: 'http://aircasting.habitatmap.org'
        }
    },
    {
        schema: 'v1',
        provider: 'purpleair',
        frequency: 'minute',
        meta: {
            url: 'https://api.purpleair.com/'
        }
    }
];
