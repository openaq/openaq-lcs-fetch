const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');

const API_BASE = '/-/api/iot-platform/v1.1.0';
const APPLICATION_ID = 16;

class AurassureApi {
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
            'pm2.5': ['pm25', 'ug/m3'],
            'pm10': ['pm10', 'ug/m3'],
            'o3': ['o3', 'ppb'],
            'so2': ['so2', 'ppb'],
            'no2': ['no2', 'ppb'],
            'humid': ['relativehumidity', '%'],
            'temp': ['temperature', 'c']
        };
        // holder for the locations
        this.measures = new Measures(FixedMeasure);
        this.locations = [];
    }

    get accessId() {
        return this.source.accessId;
    }

    get accessKey() {
        return this.source.accessKey;
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
     * @param {object} meas
     * @param {object} measurand
     * @returns {string}
     */
    getSensorId(meas) {
        const measurand = this.measurands[meas.parameter];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${meas.parameter}`);
        }
        return `aurassure-${meas.thing_id}-${measurand.parameter}`;
    }

    getLocationId(thing) {
        return `aurassure-${thing.id}`;
    }

    normalize(meas) {
        const measurand = this.measurands[meas.type];
        return measurand.normalize_value(meas.value);
    }

    async fetchLocations() {

        const url = new URL(
            `${API_BASE}/clients/${this.source.clientId}/applications/${APPLICATION_ID}/things/list`,
            this.baseUrl
        );


        const response = await request({
            url: url.href,
            json: true,
            method: 'GET',
            headers: {
                'Accept-Encoding': 'gzip',
                'Access-Id': this.accessId,
                'Access-Key': this.accessKey
            },
            gzip: true
        });
        return response.body.things;
    }

    async fetchMeasurements(things) {
        const url = new URL(
            `${API_BASE}/clients/${this.source.clientId}/applications/${APPLICATION_ID}/things/data`,
            this.baseUrl
        );

        const now = Date.now();
        const lastHour = new Date(now);
        lastHour.setMinutes(0,0,0);


        const body = {
            'data_type': 'aggregate',
            'aggregation_period': 3600,
            'parameters': [
                'pm2.5',
                'pm10',
                'o3',
                'no2',
                'so2',
                'temp',
                'humid'
            ],
            'parameter_attributes': ['value'],
            'things': things,
            'from_time': Math.floor(lastHour.getTime() / 1000) - (4 * 60 * 60),
            'upto_time': Math.floor(lastHour.getTime() / 1000)
        };

        const response = await request({
            url: url.href,
            json: true,
            method: 'POST',
            headers: {
                'Accept-Encoding': 'gzip',
                'Access-Id': this.accessId,
                'Access-Key': this.accessKey
            },
            gzip: true,
            body
        });
        return response.body.data;
    }

    async fetchData() {
        await this.fetchMeasurands();
        const things = await this.fetchLocations();
        things.map((d) => {
            try {
                this.locations.push({
                    location: this.getLocationId(d),
                    label: d.name,
                    ismobile: false,
                    lon: d.longitude,
                    lat: d.latitude
                });
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });

        const measurements = await this.fetchMeasurements(things.map(o => o.id));
        const measurementsFlat = measurements.flatMap((measurement) =>
            Object.entries(measurement.parameter_values).map(([param, metrics]) => ({
                thing_id: measurement.thing_id,
                time: measurement.time,
                parameter: param,
                ...metrics
            }))
        );
        measurementsFlat.filter((m) => m.value).map((m) => {
            this.measures.push({
                sensor_id: this.getSensorId(m),
                measure: m.value,
                timestamp: new Date(m.time * 1000).toISOString()
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
                source: 'aurassure',
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

        // create new aurassure object
        const client = new AurassureApi(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());

        return client.summary();
    }
};
