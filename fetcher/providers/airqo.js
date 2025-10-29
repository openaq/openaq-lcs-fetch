const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');


class AirQoAPI {
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
            pm2_5: ['pm25', 'ug/m3'],
            pm10: ['pm10', 'ug/m3']
        };
        // holder for the locations
        this.measures = new Measures(FixedMeasure);
        this.locations = [];
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
    async fetchMeasurements() {
        const url = new URL('api/v2/devices/measurements/cohorts', this.baseUrl);
        url.pathname += `/${this.source.cohortId}`;
        url.searchParams.set('token', this.source.secretToken);
        const response = await request({
            url,
            json: true,
            method: 'GET',
            gzip: true
        });
        console.debug(`Found ${Object.keys(response.body.measurements).length} measurements`);
        return response.body.measurements;
    }

    /**
     * Provide a sensor based ingest id
     * @param {object} meas
     * @param {object} measurand
     * @returns {string}
     */
    getSensorId(siteId , meas) {
        const measurand = this.measurands[meas];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${meas}`);
        }
        return `airqo-${siteId}-${measurand.parameter}`;
    }

    getLocationId(loc) {
        return `airqo-${loc.site_id}`;
    }


    normalize(meas) {
        const measurand = this.measurands[meas.metric];
        return measurand.normalize_value(meas.value);
    }

    async fetchData() {

        await this.fetchMeasurands();
        const measurements = await this.fetchMeasurements();

        for (const location of measurements) {
            this.locations.push({
                location: this.getLocationId(location),
                label: location.device,
                ismobile: false,
                lon: location.deviceDetails.longitude,
                lat: location.deviceDetails.latitude
            });
            Object.keys(location).filter((o) => Object.keys(this.parameters).includes(o)).map((measurand) => {
                this.measures.push({
                    sensor_id: this.getSensorId(location.site_id, measurand),
                    measure: this.normalize({ metric: measurand, value:location[measurand].value }),
                    timestamp: location.time,
                    flags: { }
                });
            });
        }

        console.debug(`Found ${measurements.length} measurements for ${this.locations.length} locations`);

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
                source: 'airqo',
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

        // create new airqo object
        const client = new AirQoAPI(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());
        return client.summary();
    }
};
