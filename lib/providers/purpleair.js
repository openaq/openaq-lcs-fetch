'use strict';

const { promisify } = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');

async function processor(source_name, source) {
    if (!process.env.SecretPurpleAir) throw new Error('SecretPurpleAir Env Var Required');

    const stas = [];

    try {
        const locs = await locations(source);
        console.log(`ok - pulled ${locs.length} stations`);

        for (const loc of locs) {
            const sta = new SensorNode({
                sensor_node_id: loc.sensor_index,
                sensor_node_site_name: loc.location_type,
                sensor_node_geometry: [ loc.longitude, loc.latitude ]
            });

            stas.push(Providers.put_station(source_name, sta));
        }

        await Promise.all(stas);
        console.log('ok - all stations pushed');

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
