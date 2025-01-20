const  { Storage } =  require('@google-cloud/storage');

const Providers = require('../lib/providers');

const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');
const { parse } = require('csv-parse');

const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const customParseFormat = require('dayjs/plugin/customParseFormat');

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(customParseFormat);


class CPCBBucket {
    /**
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
            'CO': ['co', 'mg/m3'],
            'NO': ['no', 'ug/m3'],
            'NO2': ['no2', 'ug/m3'],
            'NOX': ['nox', 'ppb'],
            'OZONE': ['o3', 'ug/m3'],
            'O3': ['o3', 'ug/m3'],
            'O3AAQMS': ['o3', 'ug/m3'],
            'PM10':['pm10', 'ug/m3'],
            'PM25': ['pm25', 'ug/m3'],
            'PM2': ['pm25', 'ug/m3'],
            'PM2.5': ['pm25', 'ug/m3'],
            'S02': ['so2', 'ug/m3'],
            'SO2': ['so2', 'ug/m3'],
            'RH': ['relativehumidity', '%'],
            'TEMP': ['temperature', 'c'],
            'AT': ['temperature', 'c'],
            'WD': ['wind_direction', 'degree'],
            'WIND_DIRECTION': ['wind_direction', 'degree'],
            'WIND_SPEED': ['wind_speed', 'm/s'],
            'WS': ['wind_speed', 'm/s'],
            '_WIND_SPEED': ['wind_speed', 'm/s']
        };
        this.measures = new Measures(FixedMeasure);
        this.locations = [];
        this.token;
        this.storage = new Storage({
            credentials: JSON.parse(this.source.keyFile)
        });
    }


    get provider() {
        return this.source.provider;
    }

    get baseUrl() {
        return this.source.meta.url;
    }

    get bucket() {
        const bucket = this.storage.bucket(this.source.bucket);
        return bucket;
    }

    get measurementsFilename() {
        const today = dayjs().tz('Asia/Kolkata').subtract(3, 'hours').format('YYYY-MM-DD');
        const formattedDate = dayjs(today).format('YYYY-MM-DD');
        return `v1/cpcb-${formattedDate}.csv`;
    }

    get stationsFilename() {
        return 'v1/cpcb_metadata.csv';
    }

    async fetchMeasurands() {
        this.measurands = await Measurand.getIndexedSupportedMeasurands(this.parameters);
    }

    /**
     * Provide a sensor based ingest id
     * @param {number} siteId
     * @param {object} m
     * @returns {string}
     */
    getSensorId(siteId, m) {
        const measurand = this.measurands[m];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${m}`);
        }
        return `cpcb-${siteId}-${measurand.parameter}`;
    }

    getDeviceId(location) {
        return `cpcb-${location.siteId}`;
    }

    normalize(meas) {
        const measurand = this.measurands[meas.parameter];
        return measurand.normalize_value(parseFloat(meas.value));
    }

    async fetchStations() {
        const file = this.bucket.file(this.stationsFilename);
        const stations = [];
        return new Promise((resolve, reject) => {
            const s = file.createReadStream();
            s.pipe(parse({ delimiter: ',', columns: true }))
                .on('data', (data) => {
                    const siteIdKey = Object.keys(data)[0];
                    // destructuring with site_id doesnt work, even key lookup fails
                    const site_id = data[siteIdKey];
                    const { location_name, latitude, longitude } = data;
                    const station = {
                        siteId: site_id,
                        label: location_name,
                        ismobile: false,
                        lon: longitude,
                        lat: latitude
                    };
                    stations.push(station);
                })
                .on('end', () => {
                    console.info('Locations CSV file parsed successfully');
                    resolve(stations);
                })
                .on('error', (error) => {
                    console.error('Error parsing Locations CSV:', error);
                    reject;
                });
        });
    }

    async fetchMeasurements() {
        const now = dayjs().tz('Asia/Kolkata').subtract(3, 'hours');
        const file = this.bucket.file(this.measurementsFilename);
        const measurements = [];
        return new Promise((resolve, reject) => {
            file.createReadStream()
                .pipe(parse({ delimiter: ',', columns: true }))
                .on('data', (data) => {
                    const { datetime_local, site_id, parameter, value } = data;
                    const d = dayjs.tz(datetime_local, 'Asia/Kolkata').local().toDate();
                    if (d > now) {
                        if (Object.keys(this.parameters).includes(data.parameter)) {
                            const datetime = d.toISOString();
                            const measurement = {
                                datetime: datetime,
                                siteId: site_id,
                                parameter,
                                value
                            };
                            measurements.push(measurement);
                        }
                    }
                })
                .on('end', () => {
                    console.info('Measurements CSV file parsed successfully');
                    resolve(measurements);
                })
                .on('error', (error) => {
                    console.error('Error parsing CSV:', error);
                    reject;
                });
        });
    }

    async fetchData() {
        await this.fetchMeasurands();
        const stations = await this.fetchStations();
        stations.map((d) => {
            try {
                this.locations.push({
                    location: this.getDeviceId(d),
                    label: d.label,
                    ismobile: false,
                    lon: d.lon,
                    lat: d.lat
                });
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });
        const measurements = await this.fetchMeasurements();

        if (measurements.length > 0) {
            measurements.map((m) => {
                this.measures.push({
                    sensor_id: this.getSensorId(m.siteId, m.parameter),
                    measure: this.normalize(m),
                    timestamp: m.datetime,
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
                source: 'cpcb',
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
        const client = new CPCBBucket(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());

        return client.summary();
    }
};
