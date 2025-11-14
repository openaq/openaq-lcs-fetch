const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');

const dayjs = require('dayjs');
const customParseFormat = require('dayjs/plugin/customParseFormat');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(customParseFormat);

class Data354Api {
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
        this.missing_datasources = [];
        this.parameters = {
            'CO': ['co', 'mg/m3'],
            'NO2': ['no2', 'ug/m3'],
            'O3': ['o3', 'ug/m3'],
            'PM10': ['pm10', 'ug/m3'],
            'PM2_5': ['pm25', 'ug/m3'],
            'RH': ['relativehumidity', '%'],
            'T': ['temperature', 'c']
        };
        // holder for the locations
        this.measures = new Measures(FixedMeasure);
        this.locations = [];
    }


    get provider() {
        return this.source.provider;
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
     * Fetch the list of stations and convert to object for reference later
     * @returns {array} a list of datasources
     */
    async fetchStations() {
        const stationsUrl = new URL('getStations', this.source.dataUrl);
        const stations = await request({
            url: stationsUrl.href,
            json: true,
            method: 'GET'
        });
        return stations.body.filter((o) => o.deployment_flag === 1 && o.deployment_date !== '').map((o) => {
            const { station_id,station_name, latitude, longitude } = o;
            return {
                id: station_id,
                label: station_name,
                latitude: latitude,
                longitude: longitude
            };
        });
    }

    getSensorId(stationId, parameter) {
        const measurand = this.measurands[parameter].parameter;
        return `data354-${stationId}-${measurand}`;
    }

    getLocationId(loc) {
        return `data354-${loc.id}`;
    }


    async fetchMeasurements(stationName, dateString) {
        const params = Object.keys(this.parameters);
        const dataUrl =  this.source.dataUrl;
        const measurementsUrl = new URL(`${stationName}?start_date=${dateString}&aqi=false&pm_nowcast=false`, dataUrl);
        const data = await request({
            url: measurementsUrl.href,
            json: true,
            method: 'GET'
        });
        if (data.statusCode !== 200) {
            return [];
        }
        if (data.body.length > 1) {
            const measurements = data.body.map((o) => {
                let timestamp = dayjs.tz(o.timestamp, 'UTC');
                // convert to hour ending to match our system
                timestamp = timestamp.add(1, 'hour');
                const m = params.map((key) => ({
                    timestamp: timestamp.toISOString(),
                    parameter: key,
                    value: o[key]
                }));
                return (m);
            }).flat();
            return measurements;
        } else {
            return [];
        }

    }

    async fetchData() {
        await this.fetchMeasurands();
        const stations = await this.fetchStations();
        stations.map((o) => {
            try {
                this.locations.push({
                    location: this.getLocationId(o),
                    label: o.label,
                    ismobile: false,
                    lon: o.longitude,
                    lat: o.latitude
                });
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });

        for (const station of stations) {
            const { id,label } = station;
            console.debug(`Fetching measurements for station ${id} ${label}`);
            const now = new Date();
            const day = now.getUTCDate();
            const month = now.getUTCMonth() + 1;
            const year = now.getUTCFullYear();
            const dayPadded = String(day - 1).padStart(2, '0');
            const monthPadded = String(month).padStart(2, '0');
            const hour = now.getHours();
            const hourPadded = String(hour).padStart(2, '0');
            const dateString = `${year}-${monthPadded}-${dayPadded} ${hourPadded}:00:00`;
            const measurements = await this.fetchMeasurements(label, dateString);
            try {
                measurements.filter((o) => o.value !== '').map((m) => {
                    this.measures.push({
                        sensor_id: this.getSensorId(id, m.parameter),
                        measure: Number(m.value),
                        timestamp: m.timestamp,
                        flags: { }
                    });
                });
            } catch (e) {
                console.error(`Error adding measurement: ${e.message}`);
            }
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
                source: 'data354',
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
        const client = new Data354Api(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());

        return client.summary();
    }
};

