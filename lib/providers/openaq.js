'use strict';

const {promisify} = require('util');
const request = promisify(require('request'));

async function process(source) {
    try {
        const locs = await locations(source);

        console.error(`ok - pulled ${locs.length} stations`);
        for (const loc of locs) {

        }
    } catch (err) {
        throw new Error(err)
    }
}

async function locations(source) {
    let page = 1;

    let locs = [];

    let body;
    do {
        body = (await request({
            json: true,
            method: 'GET',
            url: source.url + `/v1/locations?limit=10000&page=${page}`
        })).body;

        locs = locs.concat(body.results);
        page++;
    } while (body.results.length);

    return locs;
}

module.exports = {
    process
};
