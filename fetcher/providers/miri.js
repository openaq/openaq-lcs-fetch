const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');


class MiriAPI {
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
            pmten: ['pm10', 'ug/m3'],
            pmtwo: ['pm25', 'ug/m3'],
            pressure: ['pressure', 'hpa'],
            humidity: ['relativehumidity', '%'],
            temperature: ['temperature', 'c']
        };
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

    getDates() {
        const format = (date) =>
            new Intl.DateTimeFormat('en-CA', { timeZone: 'UTC' }).format(date);

        const today = new Date();
        const yesterday = new Date(today.getTime() - 24 * 60 * 60 * 1000);

        return [
            format(today),
            format(yesterday)
        ];
    }


    async fetchDevices() {
        const url = new URL('/api/devices', this.baseUrl);
        url.searchParams.set('api_key', this.source.apiKey);
        const response = await request({
            url: url.href,
            json: true,
            method: 'GET',
            gzip: true
        });
        const devices = response.body.slice(1); // first element is a metadata object not a device
        console.debug(`Miri found ${Object.keys(devices || []).length} devices`);
        return devices;
    }


    async fetchMeasurements(deviceId) {
        const [today, yesterday] = this.getDates();
        const url = new URL('/api/air-data', this.baseUrl);
        url.searchParams.set('device', deviceId);
        url.searchParams.set('variables', 'temperature,humidity,pressure,pmtwo,pmten');
        url.searchParams.set('start-date', yesterday);
        url.searchParams.set('end-date', today);
        url.searchParams.set('api_key', this.source.apiKey);
        const response = await request({
            url: url.href,
            json: true,
            method: 'GET',
            gzip: true
        });
        const data = response.body[0].data;
        console.debug(`Miri Found ${Object.keys(data || []).length} measurements for device ${deviceId}`);
        return data;
    }

    getSensorId(siteId , meas) {
        const measurand = this.measurands[meas];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${meas}`);
        }
        return `miri-${siteId}-${measurand.parameter}`;
    }

    getLocationId(loc) {
        return `miri-${loc.device_id}`;
    }


    normalize(meas) {
        const measurand = this.measurands[meas.metric];
        return measurand.normalize_value(meas.value);
    }

    async fetchData() {

        await this.fetchMeasurands();
        const devices = await this.fetchDevices();

        for (const device of devices) {
            const [lat,lon] = device.location.split(',').map(o => Number(o));
            this.locations.push({
                location: this.getLocationId(device),
                label: device.name,
                ismobile: false,
                lon: lon,
                lat: lat
            });
            const measurements = await this.fetchMeasurements(device.device_id);
            const recent = measurements.length ? measurements.slice(-3) : [];
            for (const measurement of recent) {
                const entries = Object.entries(measurement).filter(([key]) => key in this.parameters);

                for (const [measurand, value] of entries) {
                    this.measures.push({
                        sensor_id: this.getSensorId(device.device_id, measurand),
                        measure: this.normalize({ metric: measurand, value }),
                        timestamp: measurement.date_added.replace(' ', 'T'),
                        flags: {}
                    });
                }
            }

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
                source: 'miri',
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

        // create new MiriAPI object
        const client = new MiriAPI(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());
        return client.summary();
    }
};
