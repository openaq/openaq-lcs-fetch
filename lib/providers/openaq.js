'use strict';

const { promisify } = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');

async function process(source_name, source) {
    try {
        const locs = await locations(source);

        console.log(`ok - pulled ${locs.length} stations`);
        const stas = [];

        for (const loc of locs) {
            const sta = {
                sensor_ode_id: loc.id,
                sensor_node_site_name: loc.id,
                sensor_node_site_description: undefined,
                sensor_node_deployed_by: loc.sourceType,
                sensor_node_deployed_date: loc.firstUpdated,
                sensor_node_deploy_notes: '',
                sensor_node_ismobile: false,
                sensor_node_geometry: [ loc.coordinates.longitude, loc.coordinates.latitude ],
                sensor_node_timezone: undefined,
                sensor_node_reporting_frequency: undefined,
                sensor_node_city: loc.city,
                sensor_node_country: loc.country
            };


            for (const key of Object.keys(loc)) {
                if (['id', 'country', 'city' ,'coordinates', 'firstUpdated', 'lastUpdated', 'countsByMeasurement', 'parameters'].includes(key)) continue;
                sta.meta[key] = loc[key];
            }

            for (const sensor of loc.countsByMeasurement) {
                sta.sensor_nodes.push({
                    name: sensor.parameter,
                    mobile: false,
                    systems: [{
                        name: sensor.parameter,
                        sensors: [{
                            measureand: sensor.parameter,
                            units: 'ug/m3'
                        }]
                    }]
                });
            }

            stas.push(Providers.put_station(source_name, sta));
        }

        await Promise.all(stas);
        console.log('ok - all stations pushed');

        const measures = [`"sensor_id","timestamp"`];
        locs.filter((loc) => {
            return loc.countsByMeasurement.length;
        }).forEach((loc) => {
            for (const m of loc.countsByMeasurement) {
                measures.push(`"${m.parameter}","${loc.lastUpdated}"`);
            }
        });

        await Providers.put_measures(source_name, measures);

    } catch (err) {
        throw new Error(err);
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
