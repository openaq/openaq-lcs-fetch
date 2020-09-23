'use strict';

const {promisify} = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');

async function process(source) {
    try {
        const locs = await locations(source);

        console.error(`ok - pulled ${locs.length} stations`);
        for (const loc of locs) {
            const sta = {
                id: loc.id,
                country: loc.country,
                city: loc.city,
                coordinates: [ loc.coordinates.longitude, loc.coordinates.latitude ],
                firstUpdated: loc.firstUpdated,
                lastUpdated: loc.lastUpdated
                meta: { }
            });

            for (const key of Object.keys(loc)) {
                if (['id', 'country' ,'coordinates', 'firstUpdated', 'lastUpdated'].includes(key)) continue;
                sta.meta[key] = loc[key];
            }

            await Providers.put_station(sta);
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
