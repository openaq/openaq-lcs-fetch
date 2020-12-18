'use strict';

const { promisify } = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');
const { Measures, FixedMeasure, MobileMeasure } = require('../measure');

async function processor(source_name, source) {
    await process_fixed_locations(source_name, source);
    await process_mobile_locations(source_name, source);
}

async function process_fixed_locations(source_name, source) {
    const stations = [];
    const measures = new Measures(FixedMeasure);

    const locations = await fixed_locations(source);
    console.log(`ok - pulled ${locations.length} fixed stations`);

    for (const location of locations) {
        const system = new SensorSystem();

        const sta = new SensorNode({
            sensor_node_id: location.id,
            sensor_node_site_name: location.title,
            sensor_node_geometry: [location.longitude, location.latitude],
            sensor_node_source_name: 'HabitatMap',
            sensor_node_ismobile: false,
            sensor_system: system
        });

        const sensor = new Sensor({
            sensor_id: `${sta.sensor_node_source_name}-${location.streams['AirBeam2-PM2.5'].id}-pm25`,
            measurand_parameter: 'pm25',
            measurand_unit: 'µg/m³'
        });

        system.sensors.push(sensor);
        stations.push(Providers.put_station(source_name, sta));

        if (location.streams['AirBeam2-PM2.5'].average_value === null) continue;
        measures.push({
            sensor_id: `HabitatMap-${location.streams['AirBeam2-PM2.5'].id}-pm25`,
            measure: location.streams['AirBeam2-PM2.5'].average_value,
            timestamp: location.end_time_local
        });
    }

    await Promise.all(stations);
    console.log(`ok - all ${stations.length} fixed stations pushed`);
    await Providers.put_measures(source_name, measures);
    console.log(`ok - all ${measures.length} fixed measures pushed`);
}

async function process_mobile_locations(source_name, source) {
    const stations = [];
    const measures = new Measures(MobileMeasure);

    const locations = await mobile_locations(source);
    console.log(`ok - pulled ${locations.length} mobile stations`);

    for (const location of locations) {
        if (!location.streams['AirBeam2-PM2.5']) continue;

        const system = new SensorSystem();

        const sta = new SensorNode({
            sensor_node_id: location.id,
            sensor_node_site_name: location.title,
            sensor_node_source_name: 'HabitatMap',
            sensor_node_ismobile: true,
            sensor_system: system
        });

        const sensor = new Sensor({
            sensor_id: `${sta.sensor_node_source_name}-${location.streams['AirBeam2-PM2.5'].id}-pm25`,
            measurand_parameter: 'pm25',
            measurand_unit: 'µg/m³'
        });

        system.sensors.push(sensor);
        stations.push(Providers.put_station(source_name, sta));

        const measurements = await mobile_measures(source, location.streams['AirBeam2-PM2.5'].id);
        for (const measurement of measurements) {
            measures.push(new MobileMeasure({
                sensor_id: `HabitatMap-${location.streams['AirBeam2-PM2.5'].id}-pm25`,
                measure: measurement.value,
                timestamp: measurement.time,
                longitude: measurement.longitude,
                latitude: measurement.latitude
            }));
        }
    }
    await Promise.all(stations);
    console.log(`ok - all ${stations.length} mobile stations pushed`);

    await Providers.put_measures(source_name, measures);
    console.log(`ok - all ${measures.length} mobile measures pushed`);
}

async function fixed_locations(source) {
    const params = {
        time_from: String(Math.round(Date.now() / 1000) - 60 * 2), // 60s * 2min
        time_to: String(Math.round(Date.now() / 1000)),
        tags: '',
        usernames: '',
        limit: 500,
        offset: 0,
        sensor_name: 'airbeam2-pm2.5',
        measurement_type: 'Particulate Matter',
        unit_symbol: 'µg/m³'
    };

    const url = new URL(source.meta.url + '/api/fixed/active/sessions.json');
    url.searchParams.append('q', JSON.stringify(params));

    const res = await request({
        json: true,
        method: 'GET',
        url: url
    });

    return res.body.sessions;
}

async function mobile_measures(source, station_id) {
    const url = new URL(source.meta.url + '/api/measurements.json');
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
        sensor_name: 'airbeam2-pm2.5',
        measurement_type: 'Particulate Matter',
        unit_symbol: 'µg/m³'
    };

    let locs = [];

    let fetchableSessionsCount;
    do {
        params.offset = page * 1000;

        const url = new URL(source.meta.url + '/api/mobile/sessions.json');
        url.searchParams.append('q', JSON.stringify(params));

        const res = await request({
            json: true,
            method: 'GET',
            url: url
        });

        locs = locs.concat(res.body.sessions);
        console.log(`ok - pulled batch #${params.offset}: ${res.body.sessions.length} stations`);

        fetchableSessionsCount = res.body.fetchableSessionsCount;
        ++page;
    } while (fetchableSessionsCount > params.offset);

    return locs;
}

module.exports = {
    processor
};
