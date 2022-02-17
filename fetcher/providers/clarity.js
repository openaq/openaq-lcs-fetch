/**
 * NOTES:
 *   - The Google Sheet that is associated with this provider must be explicitely shared
 *     with the Google OAuth Service Account. https://stackoverflow.com/a/49965912/728583
 */

const { google } = require('googleapis');
const dayjs = require('dayjs');
const pLimit = require('p-limit');

const Providers = require('../lib/providers');
const { fetchSecret, VERBOSE, toCamelCase, request } = require('../lib/utils');
const { Sensor, SensorNode, SensorSystem } = require('../lib/station');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');

const lookup = {
    relHumid: ['relativehumidity', '%'], // RelativeHumidity
    temperature: ['temperature', 'c'], // Temperature
    pm2_5ConcMass: ['pm25', 'μg/m3'], //	PM2.5 mass concentration
    pm1ConcMass: ['pm1', 'μg/m3'], //	PM1 mass concentration
    pm10ConcMass: ['pm10', 'μg/m3'], //	PM10 mass concentration
    no2Conc: ['no2', 'ppb'], // NO2 volume concentration
    windSpeed: ['windspeed', 'm/s'], //	Wind speed
    windDirection: ['winddirection', 'degrees'] //	Wind direction, compass degrees (0°=North, then clockwise)
};

/**
 * Fetch Clarity organizations from management worksheet
 * @param {string} spreadsheetId
 * @param {string} credentials
 * @returns {Organization[]}
 */
async function listOrganizations(spreadsheetId, credentials) {
    if (VERBOSE) console.debug(`Fetching Google sheet ${spreadsheetId}...`);
    const sheets = google.sheets({
        version: 'v4',
        auth: new google.auth.GoogleAuth({
            credentials,
            scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
        })
    });
    const res = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: "'Form Responses'!A:B"
    });

    const rows = res.data.values;
    if (VERBOSE)
        console.debug(`Retrieved ${rows.length} rows from the Google sheet`);
    if (rows.length <= 1) return [];

    const columns = rows.shift().map(toCamelCase);
    return rows.map((row) =>
        Object.assign({}, ...row.map((col, i) => ({ [columns[i]]: col })))
    );
}

class ClarityApi {
    /**
     *
     * @param {Source} source
     * @param {Organization} org
     */
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
     * @returns {Promise<Datasource[]>}
     */
    listDatasources() {
        return request({
            json: true,
            method: 'GET',
            headers: { 'X-API-Key': this.apiKey },
            url: new URL('v1/datasources', this.baseUrl)
        }).then((response) => {
            var ds = response.body;
            if(process.env.SOURCEID) {
                ds = ds.filter(d => d.deviceCode == process.env.SOURCEID);
            }
            if (VERBOSE) {
                console.log(`-------------------\nListing ${ds.length} sources for ${this.org.organizationName}`);
                ds.map(d => console.log(`${d.deviceCode} - ${d.name} - ${d.subscriptionStatus}`));
            }
            return ds;
        });
    }

    /**
     *
     * @returns {Promise<Device[]>}
     */
    listDevices() {
        return request({
            json: true,
            method: 'GET',
            headers: { 'X-API-Key': this.apiKey },
            url: new URL('v1/devices', this.baseUrl)
        }).then((response) => response.body).then((response) => {
            if(process.env.SOURCEID) {
              const sources = process.env.SOURCEID.split(",");
              const total = response.length;
              response = response.filter(d => sources.includes(d.code));
              console.debug(`Limiting sensors to ${process.env.SOURCEID}, found ${response.length} of ${total}`);
            }
            const working = response.filter((o) => o.lifeStage === 'working');
            if (VERBOSE) {
                console.debug(`-----------------\nListing devices for ${this.org.organizationName}\nFound ${response.length} total devices, ${working.length} working`);
                response
                    .filter((d) => d.lifeStage !== 'working')
                    .map((d) => console.log(`${d.code} - ${d.lifeStage}`));
            }
            return working;
        });
    }

    /**
     *
     * @returns {AugmentedDevice[]}
     */
    async listAugmentedDevices() {
        const [devices, datasources] = await Promise.all([
            this.listDevices(),
            this.listDatasources()
        ]);

        const indexedDatasources = Object.assign(
            {},
            ...datasources.map((datasource) => ({
                [datasource.deviceCode]: datasource
            }))
        );

        return devices.map((device) => ({
            ...indexedDatasources[device.code],
            ...device
        }));
    }

    /**
     *
     * @param {String} code
     * @param {dayjs.Dayjs} since
     * @param {dayjs.Dayjs} to
     * @yields {Measurement}
     */
    async *fetchMeasurements(code, since, to) {
        if (VERBOSE)
            console.log(
                `--------------------\nFetching measurements for ${this.org.organizationName}/${code} since ${since} to ${to}`
            );

        const limit = 20000;
        let offset = 0;

        const url = new URL('v1/measurements', this.baseUrl);
        url.searchParams.set('code', code);
        url.searchParams.set('limit', limit);
        url.searchParams.set('startTime', since.toISOString());
        url.searchParams.set('endTime', to.toISOString());

        while (true) {
            url.searchParams.set('skip', offset);
            if (VERBOSE) console.log(`Fetching ${url}&key=${this.apiKey}`);
            const response = await request({
                url,
                json: true,
                method: 'GET',
                headers: { 'X-API-Key': this.apiKey },
                gzip: true
            });

            if (response.statusCode !== 200) {
                console.warn(`Fetch failed (${response.statusCode}) ${response.body.Message}: ${url}`);
                break;
            }

            if (offset === 0 && response.body.length === 0) {
                console.warn(`Fetch failed to return any data: ${code}`);
                break;
            }

            for (const measurement of response.body) {
                yield measurement;
            }

            // More data to fetch
            if (response.body.length === limit) {
                offset += limit;
                continue;
            }

            if (VERBOSE)
                console.log(
                    `Got ${response.body.length} of ${limit} possible measurements for device ${code} at offset ${offset}, stopping pagination.`
                );

            break;
        }
    }

    /**
     *
     * @param {*} supportedMeasurands
     * @param {Dayjs} since
     */
    async sync(supportedMeasurands, since) {
        // get all the devices, even if expired
        var devices = await this.listAugmentedDevices();
        if (VERBOSE) console.log(`-----------------------\n Syncing ${this.source.provider}/${this.org.organizationName}`, devices.length);
        // Create one station per device
        const stations = devices.map((device) =>
            Providers.put_station(
                this.source.provider,
                new SensorNode({
                    sensor_node_id: `${this.org.organizationName}-${device.code}`,
                    sensor_node_site_name: device.name || device.code, // fall back to code when missing name
                    sensor_node_geometry: device.location.coordinates,
                    sensor_node_status: device.subscriptionStatus,
                    sensor_node_source_name: this.org.organizationName,
                    sensor_node_ismobile: false,
                    sensor_node_deployed_date: device.workingStartAt,
                    sensor_system: new SensorSystem({
                        sensor_system_manufacturer_name: this.source.provider,
                        //   Create one sensor per characteristic
                        sensors: device.enabledCharacteristics
                            .map((characteristic) => supportedMeasurands[characteristic])
                            .filter(Boolean)
                            .map(
                                (measurand) =>
                                    new Sensor({
                                        sensor_id: getSensorId(device, measurand),
                                        measurand_parameter: measurand.parameter,
                                        measurand_unit: measurand.normalized_unit
                                    })
                            )
                    })
                })
            )
        );

        if (VERBOSE) console.debug(`Fetching measurements for ${devices.length} devices`);
        // now remove the expired ones
        devices = devices.filter(d=>d.subscriptionStatus=='active');
        // Sequentially process readings for each device
        const measures = new Measures(FixedMeasure);
        var successes = 0;
        for (const device of devices) {
            let hasMeasures = 0;
            const measurements = this.fetchMeasurements(
                device.code,
                since.subtract(1.25, 'hour'),
                since
            );

            for await (const measurement of measurements) {
                const readings = Object.entries(measurement.characteristics);
                for (const [type, { value }] of readings) {
                    const measurand = supportedMeasurands[type];
                    if (!measurand) continue;
                    hasMeasures = 1;
                    measures.push({
                        sensor_id: getSensorId(device, measurand),
                        measure: measurand.normalize_value(value),
                        timestamp: measurement.time
                    });
                }
            }
            successes += hasMeasures;
        }

        if (successes < devices.length) {
            console.warn(`There were ${successes} successful requests out of ${devices.length}\n------------------------------`);
        }
        await Promise.all([
            ...stations,
            Providers.put_measures(this.source.provider, measures)
        ]);
    }
}

function getSensorId(device, measurand) {
    return `${device.code}-${measurand.parameter}`;
}

module.exports = {
    async processor(source_name, source) {
        const [credentials, measurandsIndex] = await Promise.all([
            fetchSecret(source_name),
            Measurand.getIndexedSupportedMeasurands(lookup)
        ]);

        const orgs = await listOrganizations(
            source.meta.spreadsheetId,
            credentials
        );

        const now = dayjs();
        const limit = pLimit(10); // Limit to amount of orgs being processed at any given time

        return Promise.all(
            orgs.map((org) =>
                limit(() => new ClarityApi(source, org).sync(measurandsIndex, now))
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

/**
 * @typedef {Object} Device
 *
 * @property {String} _id
 * @property {String} code
 * @property {('purchased'|'configured'|'working'|'decommisioned')} lifeStage
 * @property {String[]} enabledCharacteristics
 * @property {Object} state
 * @property {Object} location
 * @property {Boolean} indoor
 * @property {String} workingStartAt
 * @property {String} lastReadingReceivedAt
 * @property {('nominal'|'degraded'|'critical')} sensorsHealthStatus
 * @property {('needsSetup'|'needsAttention'|'healthy')} overallStatus
 */

/**
 * @typedef {Object} Datasource
 *
 * @property {String} datasourceId unique id of the datasource
 * @property {String} deviceCode The short ID of the device that produced the Measurement, usually starting with "A".
 * @property {String} sourceType A Clarity device "CLARITY_NODE" or government reference site "REFERENCE_SITE"
 * @property {String} [name] The name assigned to the data source by the organization. If the dataSource is not named, the underlying deviceCode is returned. Optional.
 * @property {String} [group] The group assigned to the data source by the organization, or null if no group. Optional.
 * @property {String[]} [tags] Identifying tages assigned to the data source by the organization. Optional.
 * @property {('active'|'expired')} subscriptionStatus
 * @property {String} subscriptionExpirationDate When the subscription to this datasource will expire
 */

/**
 * @typedef {Device | Datasource} AugmentedDevice
 */
