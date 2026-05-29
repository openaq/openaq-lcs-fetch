const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');


class Breathe2ChangeApi {
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
        this.cookieHeader;
        this.missing_datasources = [];
        this.parameters = {
            'pm1_0_ATM': ['pm1', 'ug/m3'],
            'pm2_5_ATM': ['pm25', 'ug/m3'],
            'pm10_0_ATM': ['pm10', 'ug/m3'],
            'temp': ['temperature', 'c'],
            'hum': ['relativehumidity', '%'],
            'pres': ['pressure', 'pa']
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

    async fetchToken() {
        const url = new URL('auth/signin', this.baseUrl);
        const body = {
            'formFields': [
                {
                    'id': 'email',
                    'value': this.source.email
                },
                {
                    'id': 'password',
                    'value': this.source.password
                }
            ]
        };
        const response = await request({
            url: url.href,
            json: true,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'st-auth-mode': 'cookie'
            },
            body: body
        });
        const setCookie = response.headers['set-cookie'];
        const cookieHeader = setCookie.map((c) => c.split(';')[0]).join('; ');
        this.cookieHeader = cookieHeader;
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
        const url = new URL('/v0/private/implementation/active', this.baseUrl);
        url.searchParams.set('withDetails', 'true');
        const response = await request({
            url: url.href,
            json: true,
            method: 'GET',
            gzip: true,
            headers: {
                'rid':'emailpassword',
                'st-auth-mode': 'cookie',
                cookie: this.cookieHeader
            }
        });
        console.debug(`Breathe2Change Found ${Object.keys(response.body || []).length} devices`);
        return response.body;
    }


    async fetchMeasurements(uuid, sensors) {
        const url = new URL(`/v0/private/implementation/${uuid}/data`, this.baseUrl);
        url.searchParams.set('sensors', sensors);
        url.searchParams.set('type', 'fixed');
        url.searchParams.set('aggregate','10m-avg');
        url.searchParams.set('sensors', sensors);
        url.searchParams.set('to', Math.floor(Date.now() / 1000));
        url.searchParams.set('inFormat', 'epoch');
        url.searchParams.set('outFormat', 'epoch');
        url.searchParams.set('range', '1D');
        const response = await request({
            url: decodeURIComponent(url.href),
            json: true,
            method: 'GET',
            gzip: true,
            headers: {
                'rid':'emailpassword',
                'st-auth-mode': 'cookie',
                cookie: this.cookieHeader
            }
        });
        const totalMeasurements = (response.body?.sensors || []).reduce((sum, sensor) => sum + (sensor.data || []).length, 0);
        console.debug(`Breathe2Change Found ${totalMeasurements} measurements`);
        return response.body.sensors;
    }

    getSensorId(id , meas) {
        const measurand = this.measurands[meas];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${meas}`);
        }
        return `breathe2change-${id}-${measurand.parameter}`;
    }

    getLocationId(device) {
        return `breathe2change-${device.id}`;
    }


    normalize(meas) {
        const measurand = this.measurands[meas.metric];
        return measurand.normalize_value(meas.value);
    }

    async fetchData() {
        await this.fetchMeasurands();
        const devices = await this.fetchDevices();

        const twoHoursAgo = Math.floor(Date.now() / 1000) - (2 * 60 * 60);

        for (const device of devices) {
            // Skip inactive devices
            if (device.status !== 'A') continue;
            const sensors = device.node.sensors.filter((o) => Object.keys(this.parameters).includes(o.identifier)).map(o => o.id)
            const measurements = await this.fetchMeasurements(device.id, sensors.join(','));
            const [lon, lat] = device.geo.geometry.coordinates;

            this.locations.push({
                location: this.getLocationId(device),
                label: device.name,
                ismobile: device.isMobile === 'Y',
                lon,
                lat
            });

            const sensorMeta = new Map(
                device.node.sensors.map((s) => [s.id, s])
            );

            for (const reading of measurements) {
                const sensor = sensorMeta.get(reading.id);
                if (!sensor) continue;

                const identifier = sensor.identifier;


                if (!Object.keys(this.parameters).includes(identifier)) { 
                    continue;
                }
                for (const measurement of reading.data) {

                    if (measurement.value === null) {
                        continue;
                    }

                    if (measurement.timestamp < twoHoursAgo) {
                        continue;
                    }

                    this.measures.push({
                        sensor_id: this.getSensorId(device.id, identifier),
                        measure: this.normalize({ metric: identifier, value: measurement.value }),
                        timestamp: new Date(measurement.timestamp * 1000).toISOString(),
                        flags: {}
                    });
                }
            }
        }

        console.debug(`Found ${this.measures.length} measurements for ${this.locations.length} locations`);
        this.fetched = true;
    }

    data() {
        if (!this.fetched) {
            console.warn('Data has not been fetched');
        }
        return {
            meta: {
                schema: 'v0.1',
                source: 'breathe2change',
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

        // create new Breathe2ChangeApi object
        const client = new Breathe2ChangeApi(source);

        await client.fetchToken();
        // fetch and process the data
        await client.fetchData();
        // // and then push it to the
        Providers.put_measures_json(client.provider, client.data());
        return client.summary();
    }
};
