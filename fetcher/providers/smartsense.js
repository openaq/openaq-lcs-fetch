const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');


class SmartSenseApi {
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
            'PM1': ['pm1', 'ug/m3'],
            'PM2.5': ['pm25', 'ug/m3'],
            'PM10': ['pm10', 'ug/m3'],
            'CO': ['co', 'ug/m3'],
            'SO2': ['so2', 'ug/m3'],
            'NO2': ['no2', 'ug/m3'],
            'NO': ['no', 'ug/m3'],
            '03': ['o3', 'ug/m3'],
            'T': ['t', 'c']
        };
        // holder for the locations
        this.measures = new Measures(FixedMeasure);
        this.locations = [];
    }

    get apiKey() {
        return this.source.apiKey;
    }

    get provider() {
        return this.source.provider;
    }

    get baseUrl() {
        return 'https://api.smart-airq.com/api/state';
    }

    async fetchMeasurands() {
        this.measurands = await Measurand.getIndexedSupportedMeasurands(this.parameters);
    }


    /**
     * Provide a sensor based ingest id
     * @param {object} meas
     * @param {object} measurand
     * @returns {string}
     */
    getSensorId(meas, uid) {
        const measurand = this.measurands[meas.type];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${meas.type}`);
        }
        return `smartsense-${uid}-${measurand.parameter}`;
    }

    getLocationId(loc) {
        return `smartsense-${loc.uid}`;
    }

    normalize(meas) {
        const measurand = this.measurands[meas.type];
        return measurand.normalize_value(meas.value);
    }

    async fetchData() {
        const url = `${this.baseUrl}?key=${this.apiKey}`;

        await this.fetchMeasurands();


        const response = await request({
            url,
            json: true,
            method: 'GET',
            headers: {
                'Accept-Encoding': 'gzip'
            },
            gzip: true
        });

        // console.debug(`Found ${measurements.length} measurements for ${gateways.length} gateways`);

        // translate the dataources to locations
        response.body.gateways.map((d) => {
            try {
                this.locations.push({
                    location: this.getLocationId(d),
                    label: d.name,
                    ismobile: false,
                    lon: d.location.longitude,
                    lat: d.location.latitude
                });
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });


        response.body.gateways.forEach((gateway) => {
            const acceptsParameters = gateway.things.filter((o) => Object.keys(this.measurands).indexOf(o.type) > -1);
            const validMeasures = acceptsParameters.filter((o) => o.value !== 'n/a');
            validMeasures.forEach((o) => {
                let measure;
                if (o.value === 'inv') {
                    measure = -999;
                } else {
                    measure = this.normalize(o);
                }
                this.measures.push({
                    sensor_id: this.getSensorId(o, gateway.uid),
                    measure: measure,
                    timestamp: new Date(o.timestamp).toISOString()
                });
            });
        });
        this.fetched = true;
    }

    data() {
        if (!this.fetched) {
            console.warn('Data has not been fetched');
        }
        return {
            meta: {
                schema: 'v0.1',
                source: 'smartsense',
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
        const client = new SmartSenseApi(source);
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
