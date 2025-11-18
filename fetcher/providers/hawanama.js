const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');


class HawanamaApi {
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
        this.locations = {};
        this.missing_datasources = [];
        this.parameters = {
            pm25: ['pm25', 'ug/m3']
        };
        // holder for the locations
        this.measures = new Measures(FixedMeasure);
        this.locations = [];
    }

    get provider() {
        return this.source.provider;
    }

    get baseUrl() {
        return this.source.baseUrl;
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
     * Fetch the list of locations and convert to object for reference later
     * @returns {array} a list of locations
     */
    async fetchLocations() {
        const url = new URL('locations', this.baseUrl);
        const response = await request({
            url: url.href,
            json: true,
            method: 'GET',
            gzip: true
        });
        console.debug(`Found ${Object.keys(response.body.results).length} locations`);
        return response.body.results;
    }


    async fetchMeasurements() {
        const measurementsUrl = new URL('measurements', this.baseUrl);
        const res = await request({
            url: measurementsUrl.href,
            json: true,
            method: 'GET'
        });
        if (res.statusCode !== 200) {
            return [];
        }
        return res.body.results;
    }

    /**
     * Provide a sensor based ingest id
     * @param {object} meas
     * @param {object} measurand
     * @returns {string}
     */
    getSensorId(meas) {
        const measurand = this.measurands[meas.parameter];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${meas.parameter}`);
        }
        return `hawanama-${meas.location_id}-${measurand.parameter}`;
    }

    getLocationId(loc) {
        return `hawanama-${loc.location_id}`;
    }

    normalize(meas) {
        const measurand = this.measurands[meas.parameter];
        return measurand.normalize_value(meas.value);
    }

    async fetchData() {
        await this.fetchMeasurands();
        const locations = await this.fetchLocations();
        const measurements = await this.fetchMeasurements();


        console.debug(`Found ${measurements.length} measurements for ${locations.length} locations`);
        
        locations.map((d) => {
            try {
                this.locations.push({
                    location: this.getLocationId(d),
                    label: d.location,
                    ismobile: false,
                    lon: d.lon,
                    lat: d.lat
                });
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });
        console.log(this.locations)

        measurements.map( (m) => {
            try {
                this.measures.push({
                    sensor_id: this.getSensorId(m),
                    measure: this.normalize(m),
                    timestamp: m.datetime
                });
            } catch (e) {
                console.warn(`Error adding measurement: ${e.message}`);
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
                source: 'hawanama',
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

        // create new hawanama object
        const client = new HawanamaApi(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());

        return client.summary();
    }
};
