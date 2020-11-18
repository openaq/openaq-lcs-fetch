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
                if (loc.streams['AirBeam2-PM2.5'].average_value === null) return;
                measures.push(`"HabitatMap-${loc.streams['AirBeam2-PM2.5'].id}-pm25","${loc.streams['AirBeam2-PM2.5'].average_value}"`);
            });

            await Providers.put_measures(source_name, measures);
        }
    } catch (err) {
        throw new Error(err);
    }

    try {
        const stas = [];

        const locs = await mobile_locations(source);
        console.log(`ok - pulled ${locs.length} mobile stations`);

        for (const loc of locs) {
            if (!loc.streams['AirBeam2-PM2.5']) continue;

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

        const measures = [`"sensor_id","measure","lon","lat","timestamp"`];

        for (const loc of locs) {
            const ms = await mobile_measures(source, loc.streams['AirBeam2-PM2.5'].id)

            for (const m of ms) {
                measures.push(`"HabitatMap-${loc.streams['AirBeam2-PM2.5'].id}-pm25","${m.value}","${m.longitude}","${m.latitude}","${m.time}"`);
            }
        }

        await Providers.put_measures(source_name, measures);
    } catch (err) {
        throw err;
    }
}

async function fixed_locations(source) {
    const params = {
        time_from: String(Math.round(Date.now() / 1000) - 60 * 2), // 60s * 2min
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

async function mobile_measures(source, station_id) {
    const url = new URL(source.url + '/api/measurements.json');
    url.searchParams.append('start_time', Math.round(Date.now() / 1000) - 60 * 2);
    url.searchParams.append('stream_ids', station_id);

    const res = await request({
        json: true,
        method: 'GET',
        url: url
    });

    return res.body;
}

async function mobile_locations(source) {
    let page = 0;
    const params = {
        time_from: String(Math.round(Date.now() / 1000) - 60 * 2), // 60s * 2min
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

    //do {
    params.offset = page * 1000;

    const url = new URL(source.url + '/api/mobile/sessions.json');
    url.searchParams.append('q', JSON.stringify(params));

    res = await request({
        json: true,
        method: 'GET',
        url: url
    });

    locs = locs.concat(res.body.sessions);


    console.log(`ok - pulled batch #${params.offset}: ${res.body.sessions.length} stations`)

    ++page;

    // multiple pages of MOBILE data is disabled until we figure out paging
    //} while (res.body.sessions.length === 1000)

    return locs;
}

module.exports = {
    processor
};