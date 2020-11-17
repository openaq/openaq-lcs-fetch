'use strict';

const { promisify } = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');

async function processor(source_name, source) {
    try {
        const stas = [];

        const locs = await fixed_locations(source);
        console.log(`ok - pulled ${locs.length} fixed stations`);

        for (const loc of locs) {
            const system = new SensorSystem();

            const sta = new SensorNode({
                sensor_node_id: loc.id,
                sensor_node_site_name: loc.title,
                sensor_node_geometry: [ loc.longitude, loc.latitude ],
                sensor_node_source_name: 'HabitatMap',
                sensor_node_ismobile: false,
                sensor_system: system
            });

            const sensor = new Sensor({
                sensor_id: `${sta.sensor_node_source_name}-${loc.streams['AirBeam2-PM2.5'].id}-pm25`,
                measurand_parameter: 'pm25',
                measurand_unit: 'µg/m³'
            });

            system.sensors.push(sensor);

            stas.push(Providers.put_station(source_name, sta));
        }

        if (stas.length) await Promise.all(stas);
        console.log('ok - all fixed stations pushed');

        if (locs.length) {
            const measures = [`"sensor_id","measure"`];
            locs.forEach((loc) => {
                console.log(loc.streams['AirBeam2-PM2.5'])
                if (loc.streams['AirBeam2-PM2.5'].average_value === null) return;
                measures.push(`"HabitatMap-${loc.streams['AirBeam2-PM2.5'].id}-pm25","${loc.streams['AirBeam2-PM2.5'].average_value}"`);
            });

            await Providers.put_measures(source_name, measures);
        }
    } catch (err) {
        throw new Error(err);
    }

    return; // MOBILE data is disabled until we figure out paging

    try {
        const stas = [];

        const locs = await mobile_locations(source);
        console.log(`ok - pulled ${locs.length} mobile stations`);

        for (const loc of locs) {
            const system = new SensorSystem();

            const sta = new SensorNode({
                sensor_node_id: loc.id,
                sensor_node_site_name: loc.title,
                sensor_node_source_name: 'HabitatMap',
                sensor_node_ismobile: true,
                sensor_system: system
            });

            const sensor = new Sensor({
                sensor_id: `${sta.sensor_node_source_name}-${loc.streams['AirBeam2-PM2.5'].id}-pm25`,
                measurand_parameter: 'pm25',
                measurand_unit: 'µg/m³'
            });

            system.sensors.push(sensor);

            stas.push(Providers.put_station(source_name, sta));
        }

        await Promise.all(stas);
        console.log('ok - all mobile stations pushed');
    } catch (err) {
        throw err;
    }
}

async function fixed_locations(source) {
    const params = {
        time_from: String(Math.round(Date.now() / 1000) - 60 * 15), // 60s * 15min
        time_to: String(Math.round(Date.now() / 1000)),
        tags: '',
        usernames: '',
        limit: 500,
        offset: 0,
        sensor_name: "airbeam2-pm2.5",
        measurement_type: "Particulate Matter",
        unit_symbol: "µg/m³"
    };

    const url = new URL(source.url + '/api/fixed/active/sessions.json');
    url.searchParams.append('q', JSON.stringify(params));

    const res = await request({
        json: true,
        method: 'GET',
        url: url
    });

    return res.body.sessions;
}

async function mobile_locations(source) {
    let page = 0;
    const params = {
        time_from: String(Math.round(Date.now() / 1000) - 600000000000),
        time_to: String(Math.round(Date.now() / 1000)),
        tags: '',
        usernames: '',
        limit: 1000,
        offset: 0,
        west: -179.016741991,
        east: 179.303570509,
        south: -179.00001,
        north: 179.000001,
        sensor_name: "airbeam2-pm2.5",
        measurement_type: "Particulate Matter",
        unit_symbol: "µg/m³"
    };

    let locs = [];
    let res;

    do {
        params.offset = page * 1000;
        console.error(params.offset)

        const url = new URL(source.url + '/api/mobile/sessions.json');
        url.searchParams.append('q', JSON.stringify(params));

        res = await request({
            json: true,
            method: 'GET',
            url: url
        });

        console.error(res.body)
        locs = locs.concat(res.body.sessions);


        console.error(`ok - pulled batch #${params.offset}: ${res.body.sessions.length} stations`)

        ++page;
    } while (res.body.sessions.length === 1000)

    return locs;
}

module.exports = {
    processor
};
