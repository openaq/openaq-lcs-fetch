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
                sensor_node_id: loc.sensor_index,
                sensor_node_site_name: loc.location_type,
                sensor_node_geometry: [ loc.longitude, loc.latitude ],
                sensor_node_ismobile: false,
                sensor_system: system
            });

            for (const cnt of Object.keys(lookup)) {
                if (loc[cnt] === null) continue;

                const sensor = new Sensor({
                    sensor_id: `${loc.sensor_index}-${cnt}`,
                    measurand_parameter: lookup[cnt][0],
                    measurand_unit: lookup[cnt][1]
                });

                system.sensors.push(sensor);
            }

            stas.push(Providers.put_station(source_name, sta));
        }

        await Promise.all(stas);
        console.log('ok - all stations pushed');

        const measures = [`"sensor_id","measure"`];
        locs.forEach((loc) => {
            for (const m of Object.keys(lookup)) {
                measures.push(`"${loc.sensor_index}-${m}","${loc[m]}"`);
            }
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

    console.error(res.body);
}

module.exports = {
    processor
};
