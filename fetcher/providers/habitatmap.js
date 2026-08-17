const Providers = require('../lib/providers');
const { request, checkResponseData } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');

/**
 * Bounding box for Nigeria. The sessions endpoint silently ignores the box
 * unless all four coordinates are supplied, so they travel together.
 */
const NIGERIA_BBOX = {
    west: 2.6,
    east: 14.7,
    south: 4.2,
    north: 13.9
};

/**
 * Habitmap / AirCasting stores times without timezones as 
 * UTC epoch, so correct times are ahead of "real" time by the
 * station's UTC offset. Africa/Lagos is UTC+1 year round.
 */
const NIGERIA_UTC_OFFSET_SECONDS = 60 * 60;

class HabitatMapApi {
    /**
     *
     * @param {Source} source
     */
    constructor(source) {
        this.fetched = false;
        this.source = source;
        this.measurands = null;
        // From https://github.com/HabitatMap/AirCasting/blob/master/app/models/sensor.rb#L2
        this.parameters = {
            'AirBeam2-PM2.5': ['pm25', 'µg/m³'],
            'AirBeam3-PM2.5': ['pm25', 'µg/m³'],
            'AirBeamMini-PM2.5': ['pm25', 'µg/m³'],
            'AirBeam-PM2.5': ['pm25', 'µg/m³'],
            'AirBeam-PM': ['pm25', 'µg/m³']
        };
        this.windowSeconds = 60 * 60 * 2;

        this.locations = [];
        this.measures = new Measures(FixedMeasure);
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
     * Start at 00:00 and end at 23:59 to deal with their API weirdness
     *
     * @returns {object} time_from and time_to in whole epoch seconds
     */
    sessionWindow() {
        const DAY = 60 * 60 * 24;
        const midnight = Math.floor(Date.now() / 1000 / DAY) * DAY;
        return { time_from: midnight - DAY, time_to: midnight + DAY - 60 };
    }

    /**
     * Fetch the list of active fixed (stationary) sessions within Nigeria
     * @returns {array} a list of sessions/locations
     */
    async fetchLocations() {
        const { time_from, time_to } = this.sessionWindow();
        const params = {
            time_from: String(time_from),
            time_to: String(time_to),
            tags: '',
            usernames: '',
            ...NIGERIA_BBOX,
            // sensor_name: 'airbeam3-pm2.5'
            sensor_name: 'airbeam-pm2.5',
            measurement_type: 'Particulate Matter',
            unit_symbol: 'µg/m³',
            is_indoor: 'false'
        };

        const url = new URL('/api/fixed/active/sessions.json', this.baseUrl);
        url.searchParams.append('q', JSON.stringify(params));

        const res = await request({
            json: true,
            method: 'GET',
            url: url
        });

        const locations = res.body.sessions || [];
        console.debug(`Found ${locations.length} fixed locations in Nigeria`);
        return locations;
    }

    /**
     * @param {number} stream_id
     * @param {number} start_time seconds
     * @param {number} end_time seconds
     * @returns {array}
     */
    async fetchMeasurements(stream_id, start_time, end_time) {
        const url = new URL('/api/v3/fixed_measurements/', this.baseUrl);
        url.searchParams.append('stream_id', stream_id);
        url.searchParams.append('start_time', start_time * 1000);
        url.searchParams.append('end_time', end_time * 1000);

        const res = await request({
            json: true,
            method: 'GET',
            url: url
        });

        const measurements = checkResponseData(res.body, start_time, end_time);
        if (!measurements || !measurements.length) return [];

        return measurements;
    }

    getLocationId(location) {
        return `habitatmap-${location.id}`;
    }

    getSensorId(location, streamId, param) {
        const measurand = this.measurands[param];
        if (!measurand) {
            throw new Error(`Could not find measurand for ${param}`);
        }
        return `${this.getLocationId(location)}-${measurand.parameter}`;
    }

    async fetchData() {
        await this.fetchMeasurands();
        const locations = await this.fetchLocations();

        const now = Math.round(Date.now() / 1000);
        // The API filters on its local-as-UTC column, so shift the requested
        // window forward by the station offset. 
        const start_time = now - this.windowSeconds + NIGERIA_UTC_OFFSET_SECONDS;
        const end_time = now + NIGERIA_UTC_OFFSET_SECONDS;

        for (const location of locations) {
            try {
                this.locations.push({
                    location: this.getLocationId(location),
                    label: location.title,
                    ismobile: false,
                    lon: location.longitude,
                    lat: location.latitude
                });

                for (const param of Object.keys(this.parameters)) {
                    const stream = location.streams[param];
                    if (!stream) continue;

                    const measurements = await this.fetchMeasurements(stream.id, start_time, end_time);
                    if (!measurements || measurements.length === 0) continue;

                    const measurand = this.measurands[param];

                    for (const m of measurements) {
                        this.measures.push({
                            sensor_id: this.getSensorId(location, stream.id, param),
                            measure: measurand.normalize_value(m.value),
                            // Undo the local-as-UTC convention, then convert the
                            // millisecond timestamp into a standard ISO 8601 string
                            timestamp: new Date(
                                m.time - NIGERIA_UTC_OFFSET_SECONDS * 1000
                            ).toISOString()
                        });
                    }
                }
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
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
                source: 'habitatmap',
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
                source_name: this.provider,
                message: 'Data has not been fetched'
            };
        }
        return {
            source_name: this.provider,
            locations: this.locations.length,
            measures: this.measures.length,
            from: this.measures.from,
            to: this.measures.to
        };
    }
}

module.exports = {
    async processor(source) {
        const client = new HabitatMapApi(source);
        await client.fetchData();
        await Providers.put_measures_json(client.provider, client.data());
        return client.summary();
    }
};
