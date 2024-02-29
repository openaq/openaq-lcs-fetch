const Providers = require('../lib/providers');
const { Sensor, SensorNode, SensorSystem } = require('../lib/station');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');
const { VERBOSE, fetchSecret, request } = require('../lib/utils');

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

async function processor(source) {
    const [
        measurands,
        sensorReadings,
    ] = await Promise.all([
        Measurand.getSupportedMeasurands(lookup),
				fetchSensorData(source),
    ]);

    const stations = [];
    const measures = new Measures(FixedMeasure);

    if (VERBOSE) console.log(`ok - pulled ${sensorReadings.length} stations`);

    let readings = sensorReadings;

    if (process.env.SOURCEID) {
        readings = sensorReadings.filter((d) => d.sensor_index === process.env.SOURCEID);
        console.debug(`Limiting sensors to ${process.env.SOURCEID}, found ${sensorReadings.length}`);
    }

    for (const reading of readings) {
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
        for (const measurand of measurands) {
            const measure = reading[measurand.input_param];
            if ([undefined, null].includes(measure)) continue;

            const sensor = new Sensor({
                sensor_id: `${sensorNode.sensor_node_source_name}-${reading.sensor_index}-${measurand.input_param}`,
                measurand_parameter: measurand.parameter,
                measurand_unit: measurand.normalized_unit
            });

            system.sensors.push(sensor);
            measures.push(
                new FixedMeasure({
                    sensor_id: sensor.sensor_id,
                    measure: measurand.normalize_value(measure),
                    timestamp: reading.last_seen
                })
            );
        }
        // Upload sensor system
        stations.push(
            Providers.put_station(source.provider, sensorNode)
        );
    }

    await Promise.all(stations);
    if (VERBOSE) console.log(`ok - all ${stations.length} stations pushed`);

    await Providers.put_measures(source.provider, measures);
    if (VERBOSE) console.log(`ok - all ${measures.length} measurements pushed`);
		return { locations: stations.length, measures: measures.length, from: measures.from, to: measures.to };
}

async function fetchSensorData(source) {
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
    // if we are looking for a specific sourceid lets not limit
    if (!process.env.SOURCEID) {
        // Filter results to only include sensors modified or updated within the last number of seconds.
        url.searchParams.append('max_age', 75);
        // Filter results to only include outdoor sensors.
        url.searchParams.append('location_type', 0);
    }

    const { body: { fields, data } } = await request({
        json: true,
        method: 'GET',
        headers: { 'X-API-Key': source.apiKey },
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
