const dayjs = require('dayjs');
const pLimit = require('p-limit');

const Providers = require('../lib/providers');
const { fetchSecret, VERBOSE, request } = require('../lib/utils');
const {
    Sensor,
    SensorNode,
    SensorSystem,
} = require('../lib/station');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');

const lookup = {
    CO: ['co', 'µg/m³'],
    CO2: ['co2', 'ppm'],
    SO2: ['so2', 'µg/m³'],
    NO: ['no', 'µg/m³'],
    NO2: ['no2', 'µg/m³'],
    O3: ['o3', 'µg/m³'],
    // T: ['temperature', 'C'],
    // RH: ['relativehumidity', '%'],
    // P: ['pressure', 'kPa'],
    PM1: ['pm1', 'µg/m³'],
    'PM2.5': ['pm25', 'µg/m³'],
    PM10: ['pm10', 'µg/m³']
};

class SmartSenseApi {
    constructor(source, org) {
        this.source = source;
        this.org = org;
    }

    get apiKey() {
        return this.org.apiKey;
    }

    get baseUrl() {
        return this.source.meta.url;
    }

    /**
     *
     * @param {String} code
     * @param {dayjs.Dayjs} since
     * @param {dayjs.Dayjs} to
     * @yields {Measurement}
     */
    async fetchMeasurements() {
        const url = new URL('api/state', this.baseUrl);
        url.searchParams.set('key', this.apiKey);

        const response = await request({
            url,
            json: true,
            method: 'GET',
        });

        if (response.statusCode !== 200) {
            console.warn(`Fetch failed (${response.statusCode}): ${url}`);
            return [];
        }

        return response.body.gateways;
    }

    /**
   *
   * @param {*} supportedMeasurands
   * @param {Dayjs} since
   */
    async sync(supportedMeasurands) {
        const devices = await this.fetchMeasurements();
        if (VERBOSE)
            console.log(
                `Syncing ${this.source.provider}/${this.org.organizationName}`,
                devices.length
            );

        const stations = devices.map((device) =>
            Providers.put_station(
                this.source.provider,
                new SensorNode({
                    sensor_node_id: `${this.org.organizationName}-${device.id}`,
                    sensor_node_site_name: device.location.name || device.id,
                    sensor_node_geometry: [
                        device.location.longitude,
                        device.location.latitude,
                    ],
                    sensor_node_status: 'active', // Assuming all devices are active
                    sensor_node_source_name: this.source.provider,
                    sensor_node_site_description: this.org.organizationName,
                    sensor_node_ismobile: false, // Assuming devices are stationary
                    sensor_system: new SensorSystem({
                        sensor_system_manufacturer_name: this.source.provider,
                        sensors: device.things
                            .map((thing) => supportedMeasurands[thing.type])
                            .filter(Boolean)
                            .map(
                                (measurand) =>
                                    new Sensor({
                                        sensor_id: getSensorId(device, measurand),
                                        measurand_parameter: measurand.parameter,
                                        measurand_unit: measurand.normalized_unit,
                                    })
                            ),
                    }),
                })
            )
        );

        const measures = new Measures(FixedMeasure);
        for (const device of devices) {
            for (const thing of device.things) {
                const measurand = supportedMeasurands[thing.type];
                if (!measurand) continue;

                measures.push({
                    sensor_id: getSensorId(device, measurand),
                    measure: measurand.normalize_value(thing.value),
                    timestamp: new Date(thing.timestamp).toISOString()
                });
            }
        }

        await Promise.all([
            ...stations,
            Providers.put_measures(this.source.provider, measures)
        ]);
    }
}

function getSensorId(device, measurand) {
    return `smartsense-${device.id}-${measurand.parameter}`;
}

module.exports = {
    async processor(source_name, source) {
        const [secret, measurandsIndex] = await Promise.all([
            fetchSecret('smartsense-key'),
            Measurand.getIndexedSupportedMeasurands(lookup)
        ]);
        const now = dayjs();
        const limit = pLimit(10); // Limit to amount of orgs being processed at any given time (currently one)

        return Promise.all(
            secret.organizations.map((org) =>
                limit(() =>
                    new SmartSenseApi(source, org).sync(measurandsIndex, now)
                )
            )
        );
    }
};


/**
 * @typedef {Object} Organization
 *
 * @property {String} apiKey
 * @property {String} organizationName
 */
