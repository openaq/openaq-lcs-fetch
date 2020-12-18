'use strict';

const { promisify } = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');
const { Measures, FixedMeasure } = require('../measure');
const { fetchSecret } = require('../utils');

// dataKey: [ measurand_parameter, measurand_unit]
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
    'humidity': ['humidity', '%'],
    'temperature': ['temperature', 'f'],
    'pressure': ['pressure', 'mb'],
    'voc': ['voc', 'iaq'],
    'ozone1': ['ozone', 'ppb']
};

/**
- [ ] Ensure timestamp is correct
*/
async function processor(source_name, source) {
    const { apiKey } = await fetchSecret(source.provider);

    const locations = await fetchSensors(source, apiKey);
    const stations = [];
    const measures = new Measures(FixedMeasure);

    console.log(`ok - pulled ${locations.length} stations`);

    for (const location of locations) {
        const system = new SensorSystem();
        const sensorNode = new SensorNode({
            sensor_node_id: location.sensor_index,
            sensor_node_source_name: 'PurpleAir',
            sensor_node_site_name: location.location_type,
            sensor_node_geometry: [location.longitude, location.latitude],
            sensor_node_ismobile: false,
            sensor_system: system
        });

        // For each measurement, add a sensor to the sensor system & log measurement
        for (const lookupEntry of Object.entries(lookup)) {
            const [inputParam, [measurand_parameter, measurand_unit]] = lookupEntry;
            const measure = location[inputParam];
            if ([undefined, null].includes(measure)) continue;

            const sensor = new Sensor({
                sensor_id: `${sensorNode.sensor_node_source_name}-${location.sensor_index}-${inputParam}`,
                measurand_parameter,
                measurand_unit
            });
            system.sensors.push(sensor);
            measures.push(
                new FixedMeasure({
                    sensor_id: sensor.sensor_id,
                    measure,
                    timestamp: Math.floor(new Date() / 1000) // TODO: This should be time measurement was recorded, not when scraped
                })
            );
        }
        // Upload sensor system
        stations.push(
            Providers.put_station(source_name, sensorNode)
        );
    }

    await Promise.all(stations);
    console.log(`ok - all ${stations.length} stations pushed`);

    await Providers.put_measures(source_name, measures);
    console.log(`ok - all ${measures.length} measurements pushed`);
}

async function fetchSensors(source, apiKey) {
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
    ];

    // https://api.purpleair.com/#api-sensors-get-sensors-data
    const url = new URL('/v1/sensors', source.meta.url);
    url.searchParams.append('fields', fields.join(','));
    url.searchParams.append('max_age', 75); // Filter results to only include sensors modified or updated within the last number of seconds.

    const res = await request({
        json: true,
        method: 'GET',
        headers: { 'X-API-Key': apiKey },
        url: url
    });
    // TODO: Get timestamp from response

    return res.body.data.map(
        // Convert array of readings to object mapping field names to readings
        (arr) => arr.reduce(
            (previous, val, index) => ({
                ...previous,
                [fields[index]]: val
            }),
            {}
        )
    );
}

module.exports = {
    processor
};
