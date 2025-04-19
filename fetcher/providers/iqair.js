const Providers = require('../lib/providers');
const { request } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');

const { parse } = require('csv-parse/sync');

const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const customParseFormat = require('dayjs/plugin/customParseFormat');

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(customParseFormat);

const stationsMap = [
    // Permian Health
    { stationId: '32934920d734e564c0f09423a2c8dcbf', name: 'Permian Health- Bertil Harding Hwy' },
    { stationId: '073cce180ca0760978ed594cd06b9f4d', name: 'Permian Health- Jaliba Junction' },
    { stationId: '8405a2f8fa5a0b7fbb110b57964cf56f', name: 'Bakoteh Landfill' },
    { stationId: '14cc270eefe11f45cc11b9b6972f7b4e', name: 'Westfield Youth Monument' },
    { stationId: '41578413e02e9102edea151794abc022', name: 'McCarthy Square, Banjul' },
    { stationId: '85a514d203254b6166f501a19f9d30c8', name: 'Permian Health/NEA Soma, Lower River Region' },
    { stationId: '1d27d3fb1eea0ab56a1c85f91842cc2a', name: 'Farafenni, North Bank Region' },
    { stationId: '1e0859732acd432d6067e167611c6976', name: 'Brikama Ba, Central River Region' },
    { stationId: '0bcefeae54316e0950351a2ff5f12ccf', name: 'TAF City, The Gambia' },
    { stationId: 'bae2c376de96c1e06a68272b64ef85ef', name: 'Dalaba Estate- TAF Africa Global' },
    { stationId: 'a8a44d7c43fead7dc29c6be59bf2cd1a', name: 'Brufut Madiba Mall' },
    { stationId: 'f9e073058ac997d8ac8414ede8cb583a', name: 'Fajikunda Health Center' },
    { stationId: 'd6075ca4598ca33b8de4b2b679e709c8', name: 'Serekunda Health Center' }
]


function camelize(str) {
    return str.toLowerCase().replace(/[^a-zA-Z0-9]+(.)/g, (m, chr) => chr.toUpperCase());
}

function daysInMonth (month, year) {
    return new Date(year, month, 0).getDate();
}

class IqAir {
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
            'pm25(ug/m3)': ['pm25', 'ug/m3']
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
        const stations = [];
        const dataUrl =  this.source.dataUrl;
        for (const { stationId } of stationsMap) {
            console.debug(`Fetching ${stationId} readme`);
            const stationReadmeUrl = new URL(`${stationId}/readme.txt`,dataUrl);
            const res = await request({
                url: stationReadmeUrl.href,
                json: false,
                method: 'GET'
            });
            const body = JSON.parse(res.body);
            const stationIdentification = body['IQAir Open Data Station Meta Data']['Station Identification'];
            const data = Object.fromEntries(
                Object.entries(stationIdentification[0]).map(([key, value]) => [camelize(key), value]
                )
            );
            const { stationName, coordinates } = data;
            stations.push({
                id: stationId,
                label: stationName,
                latitude: coordinates['Latitude'],
                longitude: coordinates['Longitude']
            });
        }
        console.debug(`Found ${stations.length} stations`);
        return stations;
    }

    getSensorId(stationId) {
        return `iqair-${stationId}-pm25`;
    }

    getLocationId(loc) {
        return `iqair-${loc.id}`;
    }


    async fetchMeasurements(stationId, year, month, day) {
        const dataUrl =  this.source.dataUrl;
        const measurementsUrl = new URL(`${stationId}/${stationId}-${year}-${month}-${day}.csv`,dataUrl);
        const res = await request({
            url: measurementsUrl.href,
            json: false,
            method: 'GET'
        });
        const data = parse(res.body, { delimiter: ',', columns: true ,relax_column_count: true });
        const firstRecords = data.slice(0,6);
        return firstRecords.filter(o => o['Datetime_start(UTC)'] !== '').map((o) => {
            const datetime = dayjs.tz(o['Datetime_start(UTC)'], 'UTC').add(1, 'hours').toISOString();
            return { value: o['pm25(ug/m3)'], datetime: datetime };
        });
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
            const { id } = station;
            console.debug(`Fetching measurements for station ${id}`);
            const now = new Date();
            const day = now.getUTCDate();
            const month = now.getUTCMonth() + 1;
            const year = now.getUTCFullYear();
            const dayPadded = String(day).padStart(2, '0');
            const monthPadded = String(month).padStart(2, '0');
            const hour = now.getUTCHours();
            if (hour < 3) { // if it early in the day also fetch the previous days data to fill gaps and deal with end of hour reporting
                console.info('Early in day, fetching previous days measurements');
                let previousDayPadded;
                let previousDayMonthPadded;
                if (day === 1) {
                    previousDayPadded = daysInMonth(month - 1, year);
                    // if it is the first of month we need to find the last day of the previous month
                    previousDayMonthPadded = String(month - 1).padStart(2, '0');
                } else {
                    previousDayPadded = String(day - 1).padStart(2, '0');
                    previousDayMonthPadded = monthPadded;
                }
                const previousDayMeasurements = await this.fetchMeasurements(id, year, previousDayMonthPadded, previousDayPadded);
                try {
                    previousDayMeasurements.filter((o) => o.value !== '').map((m) => {
                        this.measures.push({
                            sensor_id: this.getSensorId(id),
                            measure: Number(m.value),
                            timestamp: m.datetime,
                            flags: { }
                        });
                    });
                } catch (e) {
                    console.error(`Error adding measurement: ${e.message}`);
                }
            }
            const measurements = await this.fetchMeasurements(id, year, monthPadded, dayPadded);
            try {
                measurements.filter((o) => o.value !== '').map((m) => {
                    this.measures.push({
                        sensor_id: this.getSensorId(id),
                        measure: Number(m.value),
                        timestamp: m.datetime,
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
                source: 'iqair',
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
        const client = new IqAir(source);
        // fetch and process the data
        await client.fetchData();
        // and then push it to the
        Providers.put_measures_json(client.provider, client.data());

        return client.summary();
    }
};

