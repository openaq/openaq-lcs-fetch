

const Providers = require('../lib/providers');
const { Sensor, SensorNode, SensorSystem } = require('../lib/station');
const { Measures, FixedMeasure, MobileMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');
const { request, checkResponseData } = require('../lib/utils');

const lookup = {
    // input_param: [measurand_parameter, measurand_unit]
    'AirBeam2-PM2.5': ['pm25', 'µg/m³']
};


async function processor(source) {
    const measurands = await Measurand.getSupportedMeasurands(lookup);
    const fixed = await process_fixed_locations(source, measurands);
    const mobile = await process_mobile_locations(source, measurands);
    return [fixed, mobile];
}

async function process_fixed_locations(source, measurands) {
    const stations = [];
    const measures = new Measures(FixedMeasure);

    const locations = await fixed_locations(source);
    console.log(locations)

    if (!locations.length) {
        console.warn('No fixed locations returned, exiting.');
        return { source_name: 'habitatmap:fixed', locations: 0, measures: 0 };
    }
    console.log(`ok - pulled ${locations.length} fixed stations`);

    for (const location of locations) {
        const system = new SensorSystem();
        const sta = new SensorNode({
            sensor_node_id: location.id,
            sensor_node_site_name: location.title,
            sensor_node_geometry: [location.longitude, location.latitude],
            sensor_node_source_name: 'HabitatMap',
            sensor_node_ismobile: false,
            sensor_system: system
        });

        for (const measurand of measurands) {
            if (!location.streams[measurand.input_param]) continue;

            const sensor_id = `${sta.sensor_node_source_name}-${location.streams[measurand.input_param].id}-${measurand.parameter}`;

            const sensor = new Sensor({
                sensor_id,
                measurand_parameter: measurand.parameter,
                measurand_unit: measurand.normalized_unit
            });
            system.sensors.push(sensor);

            const measure = location.streams[measurand.input_param].average_value;
            if (measure) continue;
            measures.push({
                sensor_id,
                measure: measurand.normalize_value(measure),
                timestamp: location.end_time_local
            });
        }

        stations.push(Providers.put_station(source.provider, sta));
    }

    await Promise.all(stations);
    await Providers.put_measures(source.provider, measures);
    return { source_name: 'habitatmap:fixed', locations: stations.length, measures: measures.length, from: measures.from, to: measures.to };
}

async function process_mobile_locations(source, measurands) {
    const stations = [];
    const measures = new Measures(MobileMeasure);

    const locations = await mobile_locations(source);
    if (!locations.length) {
        console.warn('No mobile locations returned, exiting.');
        return { source_name: 'habitatmap:mobile', locations: 0, measures: 0 };
    }
    console.log(`ok - pulled ${locations.length} mobile stations`);

    for (const location of locations) {
        const system = new SensorSystem();
        const sta = new SensorNode({
            sensor_node_id: location.id,
            sensor_node_site_name: location.title,
            sensor_node_source_name: 'HabitatMap',
            sensor_node_ismobile: true,
            sensor_system: system
        });

        for (const measurand of measurands) {
            if (!location.streams[measurand.input_param]) continue;

            const station_id = location.streams[measurand.input_param].id;
            const sensor_id = `${sta.sensor_node_source_name}-${station_id}-${measurand.parameter}`;

            const sensor = new Sensor({
                sensor_id,
                measurand_parameter: measurand.parameter,
                measurand_unit: measurand.normalized_unit
            });
            system.sensors.push(sensor);

            const measurements = await mobile_measures(source, station_id);

            for (const measurement of measurements) {
                measures.push(new MobileMeasure({
                    sensor_id,
                    measure: measurand.normalize_value(measurement.value),
                    timestamp: measurement.time,
                    longitude: measurement.longitude,
                    latitude: measurement.latitude
                }));
            }
        }
        stations.push(Providers.put_station(source.provider, sta));
    }

    await Promise.all(stations);
    await Providers.put_measures(source.provider, measures);
    return { source_name: 'habitatmap:mobile', locations: stations.length, measures: measures.length, from: measures.from, to: measures.to };
}

async function fixed_locations(source) {
    const params = {
        time_from: String(Math.round(Date.now() / 1000) - 60 * 2), // 60s * 2min
        time_to: String(Math.round(Date.now() / 1000)),
        tags: '',
        usernames: '',
        sensor_name: 'airbeam2-pm2.5',
        measurement_type: 'Particulate Matter',
        unit_symbol: 'µg/m³',
        is_indoor: false,
    };

    const url = new URL('/api/fixed/active/sessions.json', source.meta.url);
    url.searchParams.append('q', JSON.stringify(params));

    const res = await request({
        json: true,
        method: 'GET',
        url: url
    });

    return res.body.sessions.filter(d=>d.latitude!=200);
}

async function mobile_measures(source, station_id) {
    const url = new URL('/api/measurements.json', source.meta.url);
    const start_time = Math.round(Date.now() / 1000) - 60 * 2;
    const end_time = Math.round(Date.now() / 1000);
    url.searchParams.append('start_time', start_time);
    url.searchParams.append('stream_ids', station_id);

    const res = await request({
        json: true,
        method: 'GET',
        url: url
    });

    return checkResponseData(res.body, start_time, end_time);
}

async function mobile_locations(source) {
    let page = 0;
    const params = {
        time_from: String(Math.round(Date.now() / 1000) - 60 * 2), // 60s * 2min
        time_to: String(Math.round(Date.now() / 1000)),
        tags: '',
        usernames: '',
        limit: 1000,
        offset: 0,
        west: -179.016741991,
        east: 179.303570509,
        south: -179.00001,
        north: 179.000001,
        sensor_name: 'airbeam2-pm2.5',
        measurement_type: 'Particulate Matter',
        unit_symbol: 'µg/m³'
    };

    let locs = [];

    let fetchableSessionsCount;
    do {
        params.offset = page * 1000;

        const url = new URL('/api/mobile/sessions.json', source.meta.url);
        url.searchParams.append('q', JSON.stringify(params));

        const res = await request({
            json: true,
            method: 'GET',
            url: url
        });

        locs = locs.concat(res.body.sessions);
        console.log(`ok - pulled batch #${params.offset}: ${res.body.sessions.length} stations`);

        fetchableSessionsCount = res.body.fetchableSessionsCount;
        ++page;
    } while (fetchableSessionsCount > params.offset);

    return locs;
}

module.exports = {
    processor
};
