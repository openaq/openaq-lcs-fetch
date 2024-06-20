/**
 * NOTES:
 *   - The Google Sheet that is associated with this provider must be explicitely shared
 *     with the Google OAuth Service Account. https://stackoverflow.com/a/49965912/728583
 */


const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');


class ClarityApi {
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
        this.datasources = {};
        this.missing_datasources = [];
        this.parameters = {
            pm2_5ConcMassIndividual: ['pm25', 'ug/m3']
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
        return 'https://clarity-data-api.clarity.io';
    }

    async fetchMeasurands() {
        this.measurands = await Measurand.getIndexedSupportedMeasurands(this.parameters);
    }

    addToMissingDatasources(ds) {
        if (!this.missing_datasources.includes(ds.datasourceId)) {
            console.warn('Adding to missing datasources', ds);
            this.missing_datasources.push(ds.datasourceId);
        }
    }


    /**
     * Fetch the list of datasources and convert to object for reference later
     * @returns {array} a list of datasources
     */
    async fetchDatasources() {
        const url = 'https://clarity-data-api.clarity.io/v1/open/datasources';
        const response = await request({
            url,
            json: true,
            method: 'GET',
            headers: {
                'X-API-Key': this.apiKey,
                'Accept-Encoding': 'gzip'
            },
            gzip: true
        });
        // const arr = response.body.filter(d => d.dataOrigin!='Reference Site');
        // reshape to make it easier to use
        console.debug(`Found ${Object.keys(response.body.datasources).length} datasources`);
        this.datasources = response.body.datasources; // Object.assign({}, ...arr.map(item => ({[`${item.datasourceId}`]: item})));
        return this.datasources;
    }

    /**
     * Provide a sensor based ingest id
     * @param {object} meas
     * @param {object} measurand
     * @returns {string}
     */
    getSensorId(meas) {
        const measurand = this.measurands[meas.metric];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${meas.metric}`);
        }
        return `clarity-${meas.datasourceId}-${measurand.parameter}`;
    }

    getLocationId(loc) {
        return `clarity-${loc.datasourceId}`;
    }

    getLabel(loc) {
        const datasource = this.datasources[loc.datasourceId];
        if (!datasource) {
            this.addToMissingDatasources(loc.datasourceId);
            throw new Error(`Could not find datasource for ${loc.datasourceId}`);
        }
        // still return a label even if we are missing one
        return datasource.name ? datasource.name : 'Missing device name';
    }

    normalize(meas) {
        const measurand = this.measurands[meas.metric];
        return measurand.normalize_value(meas.value);
    }

    async fetchData() {
        const dsurl = '/v1/open/all-recent-measurement/pm25/individual';
        const url = `${this.baseUrl}${dsurl}`;

        await this.fetchMeasurands();
        await this.fetchDatasources();

        const response = await request({
            url,
            json: true,
            method: 'GET',
            headers: {
                'X-API-Key': this.apiKey,
                'Accept-Encoding': 'gzip'
            },
            gzip: true
        });

        const measurements = response.body.data;
        const datasources = response.body.locations;

        console.debug(`Found ${measurements.length} measurements for ${datasources.length} datasources`);

        // translate the dataources to locations
        datasources.map((d) => {
            try {
                this.locations.push({
                    location: this.getLocationId(d),
                    label: this.getLabel(d),
                    ismobile: false,
                    lon: d.lon,
                    lat: d.lat
                });
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });


        measurements.map( (m) => {
            // really seems like the measures.push method
            // should handle the sensor/ingest id
            // and the normalizing??
            try {
                this.measures.push({
                    sensor_id: this.getSensorId(m),
                    measure: this.normalize(m),
                    timestamp: m.time,
                    flags: { 'clarity/qc': m.qc }
                });
            } catch (e) {
                // console.warn(`Error adding measurement: ${e.message}`);
            }
        });

        if (this.missing_datasources.length) {
            console.warn(`Could not find details for ${this.missing_datasources.length} datasources`, this.missing_datasources);
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
                source: 'clarity',
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

        // create new clarity object
        const client = new ClarityApi(source);
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
