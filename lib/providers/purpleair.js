'use strict';

const { promisify } = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');

// key = [ measurand_parameter, measurand_unit]
const lookup = {
    'pm1.0': ['pm10', 'µg/m³'],
    'pm2.5': ['pm25', 'µg/m³'],
    'pm10.0': ['pm100', 'µg/m³'],
    '0.3_um_count': ['um003', 'pp100ml'],
    '0.5_um_count': ['um005', 'pp100ml'],
    '1.0_um_count': ['um010', 'pp100ml'],
    '2.5_um_count': ['um025', 'pp100ml'],
    '5.0_um_count': ['um050', 'pp100ml'],
    '10.0_um_count': ['um100', 'pp100ml'],
    'humidity':  ['humidity', '%'],
    'temperature': ['temperature', 'f'],
    'pressure': ['pressure', 'mb'],
    'voc': ['voc', 'iaq'],
    'ozone1': ['ozone', 'ppb']
};

async function processor(source_name, source) {
    if (!process.env.SecretPurpleAir) throw new Error('SecretPurpleAir Env Var Required');

    const stas = [];

    try {
        const locs = await locations(source);
        console.log(`ok - pulled ${locs.length} stations`);

        for (const loc of locs) {
            const system = new SensorSystem();

            const sta = new SensorNode({
                sensor_node_id: loc.sensor_index,
                sensor_node_source_name: 'PurpleAir',
                sensor_node_site_name: loc.location_type,
                sensor_node_geometry: [ loc.longitude, loc.latitude ],
                sensor_node_ismobile: false,
                sensor_system: system
            });

            for (const cnt of Object.keys(lookup)) {
                if (loc[cnt] === null) continue;

                const sensor = new Sensor({
                    sensor_id: `${sta.sensor_node_source_name}-${loc.sensor_index}-${cnt}`,
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
                measures.push(`"${sta.sensor_node_source_name}-${loc.sensor_index}-${m}","${loc[m]}"`);
            }
        });

        await Providers.put_measures(source_name, measures);
    } catch (err) {
        throw new Error(err);
    }
}

async function locations(source) {
    let locs = [];

    const fields = [
        'sensor_index',
        'location_type',
        'name',
        'latitude',
        'longitude',
        'pm1.0',
        'pm2.5',
        'pm10.0',
        '0.3_um_count',
        '0.5_um_count',
        '1.0_um_count',
        '2.5_um_count',
        '5.0_um_count',
        '10.0_um_count',
        'humidity',
        'temperature',
        'pressure',
        'voc',
        'ozone1'
    ]

    const url = new URL(source.url + '/v1/sensors');
    url.searchParams.append('fields', fields.join(','));

    const res = await request({
        json: true,
        method: 'GET',
        headers: {
            'X-API-Key': process.env.SecretPurpleAir
        },
        url: url
    });

    return res.body.data.map((e) => {
        const obj = {};
        for (let i = 0; i < e.length; i++) {
            obj[fields[i]] = e[i];
        }

        return obj;
    });
}

module.exports = {
    processor
};
