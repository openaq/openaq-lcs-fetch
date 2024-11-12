const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');
const dayjs = require('dayjs');


class LoveMyAirApi {
    /**
     *
     * @param {Source} source
     * @param {Organization} org
     */
    constructor(source) {
        this.fetched = false;
        this.source = source;
        this._measurands = null;
        this._measures = null;
        this.gateways = {};
        this.parameters = {
            'pm25': ['pm25', 'ug/m3']
        };
        this.measures = new Measures(FixedMeasure);
        this.locations = [];
        this.token;
    }

    get provider() {
        return this.source.provider;
    }

    get baseUrl() {
        return this.source.meta.url;
    }

    async fetchMeasurands() {
        this.measurands = await Measurand.getIndexedSupportedMeasurands(this.parameters);
    }


    /**
     * Provide a sensor based ingest id
     * @param {number} deviceId
     * @param {object} m
     * @returns {string}
     */
    getSensorId(deviceId, m) {
        const measurand = this.measurands[m];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${m}`);
        }
        return `lovemyairdenver-${deviceId}-${measurand.parameter}`;
    }

    getDeviceId(device) {
        return `lovemyairdenver-${device.siteId}`;
    }

    normalize(meas) {
        const measurand = this.measurands['pm25'];
        return measurand.normalize_value(parseFloat(meas.numericValue));
    }

    async fetchDevices() {
        const url = `${this.baseUrl}?partnerid=${this.source.partnerId}`;

        const response = await request({
            url,
            json: true,
            method: 'GET',
            gzip: true
        });

        return response;

    }

    async fetchMeasurements(parameterId) {
        const url = new URL(this.source.measurementsUrl);
        const params = url.searchParams;
        params.append('id', this.source.measurementsId);
        params.append('parameterid', parameterId);
        const today = dayjs().format('MM/DD/YYYY');
        const tomorrow = dayjs().add(1, 'day').format('MM/DD/YYYY');
        params.append('startdate', today);
        params.append('enddate', tomorrow);
        params.append('timezone', '0');
        const response = await request({
            url: url.href,
            json: true,
            method: 'GET',
            gzip: true
        });

        return response;

    }

    async fetchData() {

        await this.fetchMeasurands();

        const devices = await this.fetchDevices();

        devices.body.sites.map((d) => {
            try {
                this.locations.push({
                    location: this.getDeviceId(d),
                    label: d.siteName,
                    ismobile: false,
                    lon: d.lon,
                    lat: d.lat
                });
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });

        for (const d of devices.body.sites) {
            const parameters = d.parameters.filter((parameter) => {
                const allowedUnits = ['ug/m3'];
                return allowedUnits.includes(parameter.units);
            });
            for (const parameter of parameters) {
                const measurements = await this.fetchMeasurements(parameter.parameterId);
                if (measurements.body.length > 0) {
                    const lastMeasurements = measurements.body.slice(-3);
                    lastMeasurements.map((m) => {
                        this.measures.push({
                            sensor_id: this.getSensorId(d.siteId, 'pm25'),
                            measure: this.normalize(m),
                            timestamp: m.postDate,
                            flags: { }
                        });
                    });
                }

            }

        }

        this.fetched = true;
    }

    data() {
        if (!this.fetched) {
            console.warn('Data has not been fetched');
        }
        return {
            meta: {
                schema: 'v0.1',
                source: 'lovemyair-denver',
                matching_method: 'ingest-id'
            },
            measures: this.measures.measures,
            locations: this.locations
        };
    }

    summary() {
        if (!this.fetched) {
            console.warn('Data has not been fetched');
            return {
                source_name: this.source.provider,
                message: 'Data has not been fetched'
            };
        } else {
            return {
                source_name: this.source.provider,
                locations: this.locations.length,
                measures: this.measures.length,
                from: this.measures.from,
                to: this.measures.to
            };
        }
    }
}




module.exports = {
    async processor(source) {

        // create new smartsense object
        const client = new LoveMyAirApi(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());

        return client.summary();
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
 * @typedef {Object} Gateway
 *
 * @property {String} uid
 * @property {String} name
 * @property {String} longitude
 * @property {String} latitude

 */

/**
 * @typedef {Object} Datasource
 *
 * @property {String} uid unique id of the gateway
 * @property {String} deviceCode The short ID of the device that produced the Measurement, usually starting with "A".
 * @property {String} sourceType A Clarity device "CLARITY_NODE" or government reference site "REFERENCE_SITE"
 * @property {String} [name] The name assigned to the data source by the organization. If the dataSource is not named, the underlying deviceCode is returned. Optional.
 * @property {String} [group] The group assigned to the data source by the organization, or null if no group. Optional.
 * @property {String[]} [tags] Identifying tages assigned to the data source by the organization. Optional.
 * @property {('active'|'expired')} subscriptionStatus
 * @property {String} subscriptionExpirationDate When the subscription to this gateway will expire
 */

/**
 * @typedef {Device | Datasource} AugmentedDevice
 */
