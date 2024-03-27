const Providers = require('../lib/providers');
const { Sensor, SensorNode, SensorSystem } = require('../lib/station');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');
const { request } = require('../lib/utils');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const { find } = require('geo-tz');


dayjs.extend(utc);

const lookup = {
    // input_param: [measurand_parameter, measurand_unit]
    'pm01': ['pm1', 'µg/m³'],
    'pm02': ['pm25', 'µg/m³'],
    'pm10': ['pm10', 'µg/m³'],
    'pm003Count': ['um003', 'particles/cm³'],
    'rhum': ['relativehumidity', '%'],
    'atmp': ['temperature', 'c']
};


async function getDevices(source) {
    const url = new URL('/public/api/v1/world/locations/measures/current', source.meta.url);
    url.searchParams.append('token', source.token);
    const { body } = await request({
        json: true,
        method: 'GET',
        url: url
    });
    return body;
}


async function getAllSensors(source, devices) {
    return Promise.all(devices.map(async (device) => {
        const { locationId } = device;
        const sensorData = await fetchSensorData(source, locationId, source.token);
        device.measurements = getLatestReading(sensorData);
        return device;
    }));
}

function getLatestReading(sensorData) {
    // airgradient returns a running average in time beginning
    // and the data we are looking for is not always ready when we first check
    // so we are going back 3 hrs in order to cover missing data
    // if we still see gaps we can increase the lag time
    const offset = 3;
    const from = new Date();
    const to = new Date();
    from.setHours(from.getHours() - offset);
    from.setMinutes(0);
    from.setSeconds(0);
    from.setMilliseconds(0);

    // the current hour is always wrong because its a rolling average
    to.setHours(to.getHours() - 1);
    to.setMinutes(0);
    to.setSeconds(0);
    to.setMilliseconds(0);

    const params = Object.keys(lookup);
    const measurements = sensorData
        .filter((o) => {
            const now = new Date(o.date).getTime();
            return now >= from.getTime() && now <= to.getTime();
        })
        .map((o) => {
            const timestamp = new Date(o.date);
            // convert to hour ending to match our system
            timestamp.setHours(timestamp.getHours() + 1);

            const m = params.map((key) => ({
                timestamp: timestamp.toISOString(),
                parameter: key,
                value: o[key]
            }));
            return (m);
        }).flat();

    return measurements;
}

async function processor(source) {
    let devices  = await getDevices(source);
    devices = devices.filter((o) => !o.offline && o.latitude != null && o.longitude != null );

    const measurands = await Measurand.getIndexedSupportedMeasurands(lookup);
    const readings = await getAllSensors(source, devices);
    const stations = readings.map((reading) => {
        const sensorNode = new SensorNode({
            sensor_node_id: `airgradient-${reading.locationId}`,
            sensor_node_source_name: `${source.provider}`,
            sensor_node_site_name: reading.publicLocationName,
            sensor_node_geometry: [reading.longitude, reading.latitude],
            sensor_node_city: reading.city,
            sensor_node_country: reading.country,
            sensor_node_timezone: find(reading.latitude, reading.longitude)[0],
            sensor_node_ismobile: false,
            sensor_system: new SensorSystem({
                sensor_system_manufacturer_name: 'AirGradient',
                sensors: reading.measurements
                    .map((o) => { return measurands[o.parameter];})
                    .filter(Boolean)
                    .map((measurand) =>
                        new Sensor({
                            sensor_id: `airgradient-${reading.locationId}-${measurand.parameter}`,
                            measurand_parameter: measurand.parameter,
                            measurand_unit: measurand.normalized_unit
                        })
                    )
            })
        });
        return Providers.put_station(source.provider, sensorNode);
    });

    const measures = new Measures(FixedMeasure);

    readings.map((reading) => {
        if (reading.measurements) {
            reading.measurements.map((o) => {
                const measurand = measurands[o.parameter];
                (!measurand) ? [] :
                    measures.push({
                        sensor_id: `airgradient-${reading.locationId}-${measurand.parameter}`,
                        measure: measurand.normalize_value(o.value),
                        timestamp: o.timestamp
                    });
            });
        }

    });
    // console.log(`ok - all ${stations.length} stations pushed`);
    Providers.put_measures(source.provider, measures, `airgradient-${Math.floor(Date.now() / 1000)}-${Math.random().toString(36).substring(8)}`);
    // console.log(`ok - all ${measures.length} measurements pushed`);
    return { locations: stations.length, measures: measures.length, from: measures.from, to: measures.to };
}


async function fetchSensorData(source, locationId) {
    // /world/locations/{locationId}/measures/last/buckets/{period}
    const url = new URL(`/public/api/v1/world/locations/${locationId}/measures/last/buckets/60`, source.meta.url);
    url.searchParams.append('token', source.token);
    let response = {};
    const { body, statusCode } = await request({
        json: true,
        method: 'GET',
        url: url
    });
    if (statusCode === 200) {
        response = body;
        response.statusCode = 200;
    } else {
        response.statusCode = statusCode;
    }
    return response;
}


module.exports = {
    processor
};
