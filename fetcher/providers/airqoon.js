const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');


class AirQoonApi {
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
            'PM25Calibrated': ['pm25', 'ug/m3'],
            'PM10Calibrated': ['pm10', 'ug/m3'],
            'NO2UGM3CalibratedFiltered': ['no2', 'ug/m3'],
            'O3V2UGM3CalibratedFiltered': ['o3', 'ug/m3'],
            'Humidity': ['relativehumidity', '%'],
            'Pressure': ['pressure', 'pa'],
            'Temperature': ['temperature', 'c']
        };
        this.measures = new Measures(FixedMeasure);
        this.locations = [];
        this.token;
    }

    get username() {
        return this.source.username;
    }

    get password() {
        return this.source.password;
    }


    get provider() {
        return this.source.provider;
    }

    get baseUrl() {
        return this.source.meta.url;
    }

    async fetchToken() {
        const url = `${this.baseUrl}/v1/auth/login`;
        const body = {
            'Username': this.username,
            'Password': this.password
        };
        const response = await request({
            url,
            json: true,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: body
        });
        this.token = response.body.Token;
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
        return `airqoon-${deviceId}-${measurand.parameter}`;
    }

    getDeviceId(device) {
        return `airqoon-${device.Id}`;
    }

    normalize(meas) {
        const measurand = this.measurands[meas.measurand];
        return measurand.normalize_value(parseFloat(meas.Value.replace(/,/g, '')));
    }

    async fetchDevices() {
        const url = `${this.baseUrl}/v1/devices?limit=100`;

        const response = await request({
            url,
            json: true,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${this.token}`
            },
            gzip: true
        });

        return response;

    }

    async fetchMeasurements(deviceId) {
        const url = `${this.baseUrl}/v1/devices/id/${deviceId}/telemetry/hourly`;

        const response = await request({
            url,
            json: true,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${this.token}`
            },
            gzip: true
        });

        return response;

    }

    async fetchData() {

        await this.fetchMeasurands();

        const devices = await this.fetchDevices();


        devices.body.Data.map((d) => {
            try {
                this.locations.push({
                    location: this.getDeviceId(d),
                    label: d.Name,
                    ismobile: false,
                    lon: d.Location.Longitude,
                    lat: d.Location.Latitude
                });
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });

        for (const d of devices.body.Data) {
            const hourly = await this.fetchMeasurements(d.Id);
            if (hourly.statusCode !== 200) {
                continue;
            }
            const measurements = Object.keys(hourly.body).map((measurand) => ({
                measurand,
                ...hourly.body[measurand]
            })).filter((o) => Object.keys(this.parameters).includes(o.measurand));
            measurements.map((m) => {
                this.measures.push({
                    sensor_id: this.getSensorId(d.Id, m.measurand),
                    measure: this.normalize(m),
                    timestamp: m.DateTime,
                    flags: { }
                });
            });
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
                source: 'airqoon',
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
        const client = new AirQoonApi(source);
        // fetch auth token
        await client.fetchToken();
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
