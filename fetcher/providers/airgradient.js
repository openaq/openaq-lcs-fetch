const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');


class AirGradientApi {
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
        this.parameters = {
            'pm01': ['pm1', 'µg/m³'],
            'pm02': ['pm25', 'µg/m³'],
            'pm003Count': ['um003', 'particles/cm³'],
            'rhum': ['relativehumidity', '%'],
            'atmp': ['temperature', 'c']
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

    get token() {
        return this.source.token;
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
        return `${deviceId}-${measurand.parameter}`;
    }

    getDeviceId(device) {
        return `airgradient-${device.locationId}`;
    }

    normalize(parameter, value) {
        const measurand = this.measurands[parameter];
        return measurand.normalize_value(parseFloat(value));
    }

    async fetchDevices() {
        const url = new URL(this.baseUrl);
        url.pathname = '/public/api/v1/openaq/locations/measures/current';
        url.search = `token=${this.token}`;

        const response = await request({
            url: url.href,
            json: true,
            method: 'GET'
        });

        return response;

    }

    async fetchMeasurements(locationId) {
        console.info('fetch measurements');
        const url = new URL(this.baseUrl);
        url.pathname = `/public/api/v1/openaq/locations/${locationId}/measures/last/buckets/60`;
        url.search = `token=${this.token}`;
        console.info(`Fetching ${url.href}`);
        const response = await request({
            url: url.href,
            json: true,
            method: 'GET',
            gzip: true
        });

        return response;

    }

    async batch(task, items, batchSize) {
        let position = 0;
        let results = [];
        while (position < items.length) {
            const itemsForBatch = items.slice(position, position + batchSize);
            results = [...results, ...await Promise.all(itemsForBatch.map((item) => task(item)))];
            position += batchSize;
        }
        return results;
    }


    async addMeasurements(d) {
        console.info(`Fetching measurements for ${d.locationId}`);
        const start = Date.now();
        const hourly = await this.fetchMeasurements(d.locationId);
        const elapsed = (Date.now() - start) / 1000;
        console.info(`Fetching ${d.locationId} took ${elapsed}`);

        if (hourly.statusCode !== 200) {
            console.error(`Failed to fetch measurements for location ${d.locationId}`);
            return;
        }
        console.info(`Getting latest measurements for ${d.locationId}`);

        const measurements = this.getLatestReading(hourly.body);
        measurements.map((m) => {
            this.measures.push({
                sensor_id: this.getSensorId(this.getDeviceId(d), m.parameter),
                measure: this.normalize(m.parameter, m.value),
                timestamp: m.timestamp,
                flags: { }
            });
        });
    }

    getLatestReading(sensorData) {
        // airgradient returns a running average in time beginning
        // and the data we are looking for is not always ready when we first check
        // so we are going back 3 hrs in order to cover missing data
        // if we still see gaps we can increase the lag time
        const offset = 3;
        const from = new Date();
        const to = new Date();
        from.setHours(from.getHours() - offset);
        from.setMinutes(0);
        from.setSeconds(0);
        from.setMilliseconds(0);
        // the current hour is always wrong because its a rolling average
        to.setHours(to.getHours() - 1);
        to.setMinutes(0);
        to.setSeconds(0);
        to.setMilliseconds(0);
        const params = Object.keys(this.parameters);
        const measurements = sensorData
            .filter((o) => {
                const now = new Date(o.date).getTime();
                return now >= from.getTime() && now <= to.getTime();
            })
            .map((o) => {
                const timestamp = new Date(o.date);
                // convert to hour ending to match our system
                timestamp.setHours(timestamp.getHours() + 1);
                const m = params.map((key) => ({
                    timestamp: timestamp.toISOString(),
                    parameter: key,
                    value: o[key]
                }));
                return (m);
            }).flat();
        return measurements;
    }

    async fetchData() {

        await this.fetchMeasurands();
        console.info('fetching Airgradient devices...');
        const devices = await this.fetchDevices();
        const validDevices = devices.body.filter((d) => {
            if (d.latitude == null || d.longitude == null) {
                return false;
            }
            const latStr = String(d.latitude);
            const lonStr = String(d.longitude);
            const latDecimals = latStr.includes('.') ? latStr.split('.')[1].length : 0;
            const lonDecimals = lonStr.includes('.') ? lonStr.split('.')[1].length : 0;
            return latDecimals >= 3 && lonDecimals >= 3;
        });
        validDevices.map((d) => {
            try {
                this.locations.push({
                    location: this.getDeviceId(d),
                    label: d.locationName,
                    ismobile: false,
                    lon: d.longitude,
                    lat: d.latitude
                });
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });
        console.info('added locations.');

        await this.batch((d) => this.addMeasurements(d), validDevices, 20);

        this.fetched = true;
    }

    data() {
        if (!this.fetched) {
            console.warn('Data has not been fetched');
        }
        return {
            meta: {
                schema: 'v0.1',
                source: 'airgradient',
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

        // create new airgradient object
        const client = new AirGradientApi(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());

        return client.summary();
    }
};
