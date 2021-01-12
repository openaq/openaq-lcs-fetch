const { promisify } = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');
const { Measures, FixedMeasure } = require('../measure');
const { fetchSecret, measurandNormalizer, getSupportedLookups } = require('../utils');

const lookup = {
    // input_param: [measurand_parameter, measurand_unit]
    'pm1.0': ['pm1', 'µg/m³'],
    'pm2.5': ['pm25', 'µg/m³'],
    'pm10.0': ['pm10', 'µg/m³'],
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

async function processor(source_name, source) {
    const [
        supportedLookups,
        sensorReadings
    ] = await Promise.all([
        getSupportedLookups(lookup),
        fetchSecret(source.provider)
            .then(({ apiKey }) => fetchSensorData(source, apiKey))
    ]);

    const stations = [];
    const measures = new Measures(FixedMeasure);

    console.log(`ok - pulled ${sensorReadings.length} stations`);

    for (const reading of sensorReadings) {
        const system = new SensorSystem();
        const sensorNode = new SensorNode({
            sensor_node_id: reading.sensor_index,
            sensor_node_source_name: 'PurpleAir',
            sensor_node_site_name: reading.name,
            sensor_node_geometry: [reading.longitude, reading.latitude],
            sensor_node_ismobile: false,
            sensor_system: system
        });

        // For each measurement, add a sensor to the sensor system & log measurement
        for (const { input_param, measurand_parameter, measurand_unit } of supportedLookups) {
            const measure = reading[input_param];
            const normalizedMeasurand = measurandNormalizer(measurand_unit);

            if ([undefined, null].includes(measure)) continue;

            const sensor = new Sensor({
                sensor_id: `${sensorNode.sensor_node_source_name}-${reading.sensor_index}-${input_param}`,
                measurand_parameter,
                measurand_unit: normalizedMeasurand.unit
            });

            system.sensors.push(sensor);
            measures.push(
                new FixedMeasure({
                    sensor_id: sensor.sensor_id,
                    measure: normalizedMeasurand.value(measure),
                    timestamp: reading.last_seen
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

async function fetchSensorData(source, apiKey) {
    // https://api.purpleair.com/#api-sensors-get-sensors-data
    const url = new URL('/v1/sensors', source.meta.url);
    url.searchParams.append(
        'fields',
        [
            'last_seen',
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
        ].join(',')
    );
    url.searchParams.append('max_age', 75); // Filter results to only include sensors modified or updated within the last number of seconds.
    url.searchParams.append('location_type', 0); // Filter results to only include outdoor sensors.

    const { body: { fields, data } } = await request({
        json: true,
        method: 'GET',
        headers: { 'X-API-Key': apiKey },
        url: url
    });

    return data.map(
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
