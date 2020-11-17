'use strict';

const { promisify } = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');

async function processor(source_name, source) {
    const stas = [];

    try {
        const locs = await fixed_locations(source);
        console.log(`ok - pulled ${locs.length} stations`);

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

        await Promise.all(stas);
        console.log('ok - all stations pushed');

        const measures = [`"sensor_id","measure"`];
        locs.forEach((loc) => {
            console.error(loc.streams['AirBeam2-PM2.5'])
            if (loc.streams['AirBeam2-PM2.5'].average_value === null) return;
            measures.push(`"HabitatMap-${loc.streams['AirBeam2-PM2.5'].id}-pm25","${loc.streams['AirBeam2-PM2.5'].average_value}"`);
        });

        await Providers.put_measures(source_name, measures);
    } catch (err) {
        throw new Error(err);
    }
}

async function fixed_locations(source) {
    let locs = [];

    const params = {
        time_from: String(Math.round(Date.now() / 1000) - 600000000000),
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

module.exports = {
    processor
};
