const Providers = require('../lib/providers');
const { Sensor, SensorNode, SensorSystem } = require('../lib/station');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');
const { fetchSecret, request } = require('../lib/utils');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const { find } = require('geo-tz');


dayjs.extend(utc);

const lookup = {
    // input_param: [measurand_parameter, measurand_unit]
    'pm02': ['pm25', 'µg/m³'],
    'atmp': ['temperature', 'c']
};


async function getDevices(source, token) {
    const url = new URL('/public/api/v1/world/locations/measures/current', source.meta.url);
    url.searchParams.append('token', token);
    const { body } = await request({
        json: true,
        method: 'GET',
        url: url
    });
    return body;
}


async function getAllSensors(source, devices, token) {
    return Promise.all(devices.map(async (device) => {
        const { locationId } = device;
        const sensorData = await fetchSensorData(source, locationId, token);
        device.measurements = getLatestReading(sensorData);
        return device;
    }));
}

function getLatestReading(sensorData) {
    const d = new Date();
    d.setHours(d.getHours() - 1);
    d.setMinutes(0);
    d.setSeconds(0);
    d.setMilliseconds(0);
    const currentMeasurement = sensorData.filter((o) => new Date(o.date).getTime() === d.getTime());
    const timestamp = new Date(currentMeasurement[0].date);
    timestamp.setHours(timestamp.getHours() + 1); // convert to hour ending
    const params = ['date'];
    const measurements = Object.entries(currentMeasurement[0]).map(([k,v]) => {
        if (!params.includes(k)) {
            return { 'parameter': k, 'value': v, 'timestamp': timestamp.toISOString() };
        }
    }).filter((o) => o !== undefined);
    return measurements;
}

async function processor(source_name, source) {
    const { token } = await fetchSecret('airgradient');
    let devices  = await getDevices(source, token);
    devices = devices.filter((o) => !o.offline);
    devices = devices.filter((o) => o.latitude != null && o.longitude != null );
    const measurands = await Measurand.getIndexedSupportedMeasurands(lookup);
    const readings = await getAllSensors(source, devices, token);
    const stations = readings.map((reading) => {
        const sensorNode = new SensorNode({
            sensor_node_id: `airgradient-${reading.locationId}`,
            sensor_node_source_name: `${source_name}`,
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
        return Providers.put_station(source_name, sensorNode);
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
    console.log(`ok - all ${stations.length} stations pushed`);
    Providers.put_measures(source_name, measures, `airgradient-${Math.floor(Date.now() / 1000)}-${Math.random().toString(36).substring(8)}`);
    console.log(`ok - all ${measures.length} measurements pushed`);
}


async function fetchSensorData(source, locationId, token) {
    // /world/locations/{locationId}/measures/last/buckets/{period}
    const url = new URL(`/public/api/v1/world/locations/${locationId}/measures/last/buckets/60`, source.meta.url);
    url.searchParams.append('token', token);
    let response = {};
    const { body, statusCode } = await request({
        json: true,
        method: 'GET',
        url: url
    });
    if (statusCode === 200) {
        response = body;
        body.locationId = locationId;
        response.statusCode = 200;
    } else {
        response.statusCode = statusCode;
    }
    return response;
}


module.exports = {
    processor
};
