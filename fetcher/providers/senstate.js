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
    'PM10': ['pm1', 'µg/m³'],
    'PM25': ['pm25', 'µg/m³'],
    'PM40': ['pm4', 'µg/m³'],
    'PM100': ['pm10', 'µg/m³'],
    'NO2': ['no2', 'µg/m³'],
    'SO2': ['so2','µg/m³'],
    'CO': ['co','µg/m³'],
    'Humidity': ['humidity', '%'],
    'Temperature': ['temperature', 'c'],
    'Pressure': ['pressure', 'hpa']
};


async function getDevices(source) {
    const url = new URL('/devices/openaq-devices', source.meta.url);
    const { body } = await request({
        json: true,
        method: 'GET',
        url: url
    });
    return body;
}


async function getAllSensors(source, devices) {
    const to = dayjs.utc();
    const from = to.subtract(3, 'minute');
    return Promise.all(devices.map((device) => {
        return fetchSensorData(source, device, from, to);
    }));
}

async function processor(source_name, source) {
    const devices  = await getDevices(source);

    const [
        measurands,
        sensorReadings
    ] = await Promise.all([
        Measurand.getIndexedSupportedMeasurands(lookup),
        getAllSensors(source, devices)
    ]);
    const readings = sensorReadings.filter((o) => o.statusCode === 200);
    const stations = [];
    for (const reading of readings) {
        const sensorNode = new SensorNode({
            sensor_node_id: `senstate-${reading.token}`,
            sensor_node_source_name: `${reading.attribution.name} ${reading.attribution.url}`,
            sensor_node_site_name: reading.name,
            sensor_node_geometry: [reading.coordinates.longitude, reading.coordinates.latitude],
            sensor_node_city: reading.city,
            sensor_node_country: reading.country,
            sensor_node_timezone: find(reading.coordinates.latitude, reading.coordinates.longitude)[0],
            sensor_node_ismobile: false,
            sensor_system: new SensorSystem({
                sensor_system_manufacturer_name: 'Senstate',
                sensors: reading.measurements
                    .map((o) => { return measurands[o.parameters.parameter];})
                    .filter(Boolean)
                    .map((measurand) =>
                        new Sensor({
                            sensor_id: `${reading.token}-${measurand.parameter}`,
                            measurand_parameter: measurand.parameter,
                            measurand_unit: measurand.normalized_unit
                        })
                    )
            })
        });
        stations.push(
            Providers.put_station(source_name, sensorNode)
        );
    }


    const measures = new Measures(FixedMeasure);

    sensorReadings.map((reading) => {
        reading.measurements.flatMap((o) => {
            const measurand = measurands[o.parameters.parameter];
            (!measurand) ? [] :
                measures.push({
                    sensor_id: `${reading.token}-${measurand.parameter}`,
                    measure: measurand.normalize_value(o.parameters.value),
                    timestamp: o.date.utc
                });
        });
    });

    await Promise.all(stations);
    console.log(`ok - all ${stations.length} stations pushed`);

    await Providers.put_measures(source_name, measures, `senstate-${Math.floor(Date.now() / 1000)}-${Math.random().toString(36).substring(8)}`);
    console.log(`ok - all ${measures.length} measurements pushed`);
}


async function fetchSensorData(source, token, from, to) {
    // https://open-data.senstate.cloud/devices/data-records-period/{TOKEN}
    const url = new URL(`/devices/data-records-period/${token}`, source.meta.url);
    url.searchParams.append('period', 'c');
    url.searchParams.append('startDate', from.format().slice(0, -1)); // remove trailing 'Z' match senstate API need
    url.searchParams.append('endDate', to.format().slice(0, -1));
    let response = {};
    const { body, statusCode } = await request({
        json: true,
        method: 'GET',
        url: url
    });
    if (statusCode === 200) {
        response = body;
        body.token = token;
        response.statusCode = 200;
    } else {
        response.statusCode = statusCode;
    }
    return response;
}


module.exports = {
    processor
};
