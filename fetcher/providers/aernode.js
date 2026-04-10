const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');


class AernodeAPI {
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
            'PM2.5': ['pm25', 'ug/m3'],
            PM10: ['pm10', 'ug/m3'],
            'RH': ['relativehumidity', '%'],
            'T-ext': ['temperature', 'c'],
            hPa: ['pressure', 'hpa'], // reported in hPa to convert tp pa
            NO2_ug: ['no2', 'ug/m3'],
            O3_ug: ['o3', 'ug/m3']
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

    async fetchDevices() {
        const url = new URL('/api/v1/devices/getmetadata', this.baseUrl);
        url.searchParams.set('apikey', this.source.apiKey);
        const response = await request({
            url: url.href,
            json: true,
            method: 'GET',
            gzip: true
        });
        console.debug(`Aernode Found ${Object.keys(response.body?.data || []).length} devices`);
        return response.body.data;
    }


    async fetchMeasurements(deviceId) {
        const url = new URL('/api/v1/telemetries/getdata', this.baseUrl);
        url.searchParams.set('device_id', deviceId);
        url.searchParams.set('datatype', 'adjusted');
        url.searchParams.set('apikey', this.source.apiKey);
        const response = await request({
            url: url.href,
            json: true,
            method: 'GET',
            gzip: true
        });
        console.debug(`Aernode Found ${Object.keys(response.body?.measurements || []).length} measurements for device ${deviceId}`);
        return response.body.data;
    }

    getSensorId(siteId , meas) {
        const measurand = this.measurands[meas];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${meas}`);
        }
        return `aernode-${siteId}-${measurand.parameter}`;
    }

    getLocationId(device) {
        return `aernode-${device.device_id}`;
    }


    normalize(meas) {
        const measurand = this.measurands[meas.metric];
        return measurand.normalize_value(meas.value);
    }

    async fetchData() {

        await this.fetchMeasurands();
        const devices = await this.fetchDevices();

        for (const device of devices) {
            this.locations.push({
                location: this.getLocationId(device),
                label: device.shortname,
                ismobile: false,
                lon: device.lon_set,
                lat: device.lat_set
            });
            const measurements = await this.fetchMeasurements(device.device_id);
            const parametersCount = new Set(measurements.map((o) => o.metric_name)).size;
            const recentMeasurements = measurements.length > parametersCount * 3 ? measurements.slice(-(parametersCount * 3)) : measurements;
            console.log(` recentMeasurements is ${recentMeasurements.length}`);
            recentMeasurements.filter((o) => Object.keys(this.parameters).includes(o.metric_name)).map((m) => {
                this.measures.push({
                    sensor_id: this.getSensorId(device.device_id,  m.metric_name),
                    measure: this.normalize({ metric: m.metric_name, value: m.value }),
                    timestamp: m.time,
                    flags: { }
                });
            });
        }

        console.debug(`Found ${this.measures.length} measurements for ${this.locations.length} locations`);

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
                source: 'aernode',
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

        // create new AernodeAPI object
        const client = new AernodeAPI(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());
        return client.summary();
    }
};
