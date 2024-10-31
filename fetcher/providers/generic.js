const Providers = require('../lib/providers');
const { fetchFile, DRYRUN, VERBOSE  } = require('../lib/utils');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');

dayjs.extend(utc);

const truthy = (value) => {
    return [1,true,'TRUE','T','True','t','true'].includes(value);
};

/**
 * Take a string and strip any whitespace from the ends.
 * If the value is not a string it just passes throw
 * @param {*} value - the value to the stripped
 * @returns {*} - stripped value
 */
const stripWhitespace = (value) => {
    if (typeof(value) === 'string') {
        return value.replace(/^[ ]+|[ ]+$/,'');
    } else {
        return value;
    }
};

/**
 * Take a value that is meant to be used as a key and remove any characters that will
 * get in the way later when parsing for ingestion. Removes whitespace and then turns spaces to underscores
 * @param {*} value - any real value
 * @returns {*} - cleaned string
 */
const cleanKey = (value) => {
    return value && value
        .replace(/^[ ]+|[ ]+$/,'')
        .replace(/[ ]+/g,'_')
        .replace(/[^\w]/g,'')
        .toLowerCase();
};


/**
 * Remove all NA - type values from the first level of an object
 *
 * @param {*} obj - object
 * @returns {*} - object stripped of NA values
 */
const stripNulls = (obj) => {
    return Object.assign(
        {},
        ...Object.entries(obj)
            // eslint-disable-next-line no-unused-vars
            .filter(([_, v]) => ![null,NaN,'',undefined,'undefined'].includes(v))
            .map(([k, v]) => ({ [k]: v }))
    );
};


/**
 * Assign values to a class. Will only assign values that already exists in a class and
 * all other values will be added to the metadata based on the accepted list
 * @param {*} target - Most likely the class object (this)
 * @param {*} data - The values that we are assiging to the target
 * @param {*[]} [accepted=[]] - I list of keys that will be accepted to the metadata
 */
const classAssign = (target, data, accepted = []) => {
    const keys = Object.keys(target);
    for ( const [k,v] of Object.entries(data)) {
        if (keys.includes(k)) {
            target[k] = stripWhitespace(v);
        } else if (accepted.includes(k)) {
            if (!target.metadata) target.metadata = {};
            target.metadata[k] = stripWhitespace(v);
        }
    }
};


/**
 * A sensor node location
 *
 */
class Location {
    constructor(data) {
        this.location_id = data.location_id;
        this.owner = null;
        this.label = null;
        this.lat = null;
        this.lon = null;
        this.ismobile = null;
        this.metadata = null;
        this.systems = {};

        classAssign(this, data, ['project','city','state','country']);
    }

    /**
     * Get one of the sensor systems by data/key
     * If the system does not exist it will be created
     * @param {(string|object)} data - object with data or key value
     * @returns {*} - system object
     */
    getSystem(data) {
        let key;
        if (typeof(data) === 'string') {
            key = data;
            data = { system_id: key };
        } else {
            key = data.system_id;
        }
        if (!this.systems[key]) {
            this.systems[key] = new System({ ...data });
        }
        return this.systems[key];
    }

    /**
     *  Add a new sensor to a location
     * This will also add the system as well if doesnt exist
     * @param {*} sensor - object that contains all the sensor data
     * @returns {*} - a sensor object
     */
    add(sensor) {
        // first we get the system name
        // e.g. :provider/:site/:manufacturer-:model
        const sys = this.getSystem(sensor);
        return sys.add(sensor);
    }

    /**
     * Export method to convert to json
     * @returns {object} - object
     */
    json() {
        return stripNulls({
            location: this.location_id,
            label: this.label,
            lat: this.lat,
            lon: this.lon,
            ismobile: this.ismobile,
            metadata: this.metadata,
            systems: Object.values(this.systems).map((s) => s.json()),
        });
    }
}


/**
 * System object
 */
class System {
    constructor(data) {
        this.system_id = data.system_id;
        this.manufacturer_name = null;
        this.model_name = null;
        this.metadata = {};
        this.sensors = {};
        classAssign(this, data, []);
    }

    /**
     * Adds a sensor to the system
     *
     * @param {object} sensor - data to build sensor
     * @returns {object} - sensor object
     */
    add(sensor) {
        const sensor_id = sensor.sensor_id;
        if (!this.sensors[sensor_id]) {
            this.sensors[sensor_id] = new Sensor(sensor);
        }
        return this.sensors[sensor_id];
    }

    /**
     * Export method to convert to json
     * @returns {object} - object
     */
    json() {
        return stripNulls({
            system_id: this.system_id,
            manufacturer_name: this.manufacturer_name,
            model_name: this.model_name,
            sensors: Object.values(this.sensors).map((s) => s.json()),
        });
    }


}

/**
 * Flag object
 */
// flag levels info, warning, error
class Flag {
    constructor(data) {
        this.flag_id = data.flag_id || Flag.id(data);
        this.datetime_from = data.starts;
        this.datetime_to = data.ends;
        this.flag_name = data.flag;
        this.note = data.note;
        classAssign(this, data, []);
    }

    static id(data) {
        // very basic method here
        const starts = data.starts || 'infinity';
        return `${data.sensor_id}-${data.flag}::${starts}`;
    }

    /**
     * Export method to convert to json
     * @returns {object} - object
     */
    json() {
        return stripNulls({
            flag_id: this.flag_id,
            datetime_from: this.datetime_from,
            datetime_to: this.datetime_to,
            flag_name: this.flag_name,
            note: this.note,
        });
    }
}


/**
 * Sensor object
 */
class Sensor {
    constructor(data) {
        this.sensor_id = data.sensor_id;
        this.parameter = null;
        this.interval_seconds = null;
        this.version_date = null;
        this.instance = null;
        this.status = null;
        this.flags = {};
        classAssign(this, data, []);
    }

    add(f) {
        f.sensor_id = this.sensor_id;
        const flag = new Flag(f);
        this.flags[flag.flag_id] = flag;
        return flag;
    }

    /**
     * Export method to convert to json
     * @returns {object} - object
     */
    json() {
        return stripNulls({
            sensor_id: this.sensor_id,
            version_date: this.version_date,
            status: this.status,
            instance: this.instance,
            parameter: this.parameter,
            interval_seconds: this.interval_seconds,
            flags: Object.values(this.flags).map((s) => s.json()),
        });
    }
}





/**
 * Generic process used to convert file type data into our ingest format
 */
class Client {
    /**
     * @param {*} source - object with configuration data
     */
    constructor(source) {
        this.fetched = false;
        this.source = source;
        this.location_key = source.meta.location_key || 'location';
        this.label_key = source.meta.label_key || 'location';
        this.parameter_key = source.meta.parameter_key || 'parameter';
        this.value_key = source.meta.value_key || 'value';
        this.latitude_key = source.meta.latitude_key || 'lat';
        this.longitude_key = source.meta.longitude_key || 'lng';
        this.manufacturer_key = source.meta.manufacturer_key || 'manufacturer_name';
        this.model_key = source.meta.model_key || 'model_name';
        this.datetime_key = source.meta.timestamp_key || 'datetime';
        this.datetime_format =source.meta.datetime_format || 'YYYY-MM-DD HH-mm-ss';
        this.timezone = source.meta.timezone || 'UTC';
        this.datasources = {};
        this.missing_datasources = [];
        this.parameters = source.parameters || [];
        // holder for the locations
        this.measurands = null;
        this.measures = new Measures(FixedMeasure);
        this.locations = {};
        this.sensors = {};
        this.log = {}; // track errors and warnings to provide later
    }

    get provider() {
        return cleanKey(this.source.provider);
    }

    async fetchMeasurands() {
        this.measurands = await Measurand.getIndexedSupportedMeasurands(this.parameters);
    }

    /**
     * Provide a location based ingest id
     *
     * @param {object} row - data for building key
     * @returns {string} - location id key
     */
    getLocationId(row) {
        const location = cleanKey(row[this.location_key]);
        return `${this.provider}-${location}`;
    }

    /**
     * Provide a system based ingest id
     *
     * @param {object} row - data for building key
     * @returns {string} - system id key
     */
    getSystemId(row) {
        const manufacturer = cleanKey(row[this.manufacturer_key]);
        const model = cleanKey(row[this.model_key]);
        const location_id = this.getLocationId(row);
        let key = '';
        if (manufacturer && model) {
            key = `-${manufacturer}:${model}`;
        } else if (!manufacturer & !model) {
            // key = 'default';
        } else {
            key = `-${manufacturer || model}`;
        }
        return `${location_id}${key}`;
    }

    /**
     * Provide a sensor based ingest id
     *
     * @param {object} row - data for building key
     * @returns {string} - sensor id key
     */
    getSensorId(row) {
        const measurand = this.measurands[row.metric];
        const location_id = this.getLocationId(row);
        const version = cleanKey(row.version_date);
        const instance = cleanKey(row.instance);
        if (!measurand) {
            throw new Error(`Could not find measurand for ${row.metric}`);
        }
        const key = [measurand.parameter];
        if (instance) key.push(instance);
        if (version) key.push(version);
        return `${location_id}-${key.join(':')}`;
    }


    /**
     *  Create a label for this location
     *
     * @param {object} row - data to use
     * @returns {string} - label
     */
    getLabel(row) {
        return row[this.label_key];
    }


    /**
     * Get location by key
     *
     * @param {(string|object)} key - key or data to build location
     * @returns {object} - location object
     */
    getLocation(key) {
        let loc = null;
        let data = {};
        if (typeof(key) === 'object') {
            data = { ...key };
            key = this.getLocationId(data);
        }
        loc = this.locations[key];
        if (!loc) {
            loc = this.addLocation({ location_id: key, ...data });
        }

        return loc;
    }


    /**
     * Get sensor by key
     *
     * @param {(string|object)} key - key or data to build sensor
     * @returns {object} - sensor object
     */
    getSensor(key) {
        let sensor = null;
        let data = {};
        if (typeof(key) === 'object') {
            data = { ...key };
            key = this.getSensorId(data);
        }

        sensor = this.sensors[key];
        if (!sensor) {
            //sensor = this.addSensor({ sensor_id: key, ...data });
        }

        return sensor;
    }

    /**
     * Clean up a measurement value
     *
     * @param {object} meas - object with parameter info and value
     * @returns {number} - cleaned value
     */
    normalize(meas) {
        const measurand = this.measurands[meas.metric];
        return measurand.normalize_value(meas.value);
    }

    /**
     * Create a proper timestamp
     *
     * @param {*} row - data with fields to create timestamp
     * @returns {string} - formated timestamp string
     */
    getDatetime (row) {
        const dt_string = row[this.datetime_key];
        if(!dt_string) {
            throw new Error(`Missing date/time field. Looking in ${this.datetime_key}`);
        }
        const dt = dayjs.utc(dt_string, this.datetime_format);
        if(!dt.isValid()) {
            throw new Error(`A valid date could not be made from ${dt_string} using ${this.datetime_format}`);
        }
        return dt;
    }

    /**
     *
     *
     * @param {*} f -
     * @returns {*} -
     */
    fetchData (f) {
        // if its a non-json string it should be a string that represents a location
        // local://..
        // s3://
        // google://
        // if its binary than it should be an uploaded file
        // if its an object then ...
        return fetchFile(f);
    }

    logMessage(type, message, err) {
        // check if warning or error
        // if strict than throw error, otherwise just log for later
        if(!this.log[type]) this.log[type] = [];
        this.log[type].push({ message, err});
        if (VERBOSE) console.log(`${type}:`, err && err.message);
    }

    /**
     * Entry point for processing data
     *
     * @param {(string|file|object)} file - file path, object or file
     */
    async processData(file) {
        const data = await this.fetchData(file);
        if (!data) {
            throw new Error('No data was returned from file');
        }
        if (file.type === 'locations') {
            this.processLocationsData(data);
        } else if (file.type === 'sensors') {
            this.processSensorsData(data);
        } else if (file.type === 'measurements') {
            this.processMeasurementsData(data);
        } else if (file.type === 'flags') {
            this.processFlagsData(data);
        }
    }

    /**
     * Add a location to our list
     *
     * @param {object} data - location data
     * @returns {*} - location object
     */
    addLocation(data) {
        const key = this.getLocationId(data);
        if (!this.locations[key]) {
            this.locations[key] = new Location({
                location_id: key,
                label: this.getLabel(data),
                ismobile: truthy(data.ismobile),
                lon: Number(data[this.longitude_key]),
                lat: Number(data[this.latitude_key]),
                ...data,
            });
        }
        return this.locations[key];
    }

    /**
     * Process a list of locations
     *
     * @param {array} locations - list of location data
     */
    async processLocationsData(locations) {
        console.debug(`Processing ${locations.length} locations`);
        locations.map((d) => {
            try {
                this.addLocation(d);
            } catch (e) {
                console.warn(`Error adding location: ${e.message}`);
            }
        });
    }

    /**
     * Process a list of sensors
     *
     * @param {array} sensors - list of sensor data
     */
    async processSensorsData(sensors) {
        console.debug(`Processing ${sensors.length} sensors`);
        sensors.map((d) => {
            try {

                const sensor_id = this.getSensorId({
                    location: d[this.location_key],
                    metric: d[this.parameter_key],
                    ...d,
                });

                const system_id = this.getSystemId(d);
                const location = this.getLocation(d);
                // maintain a way to get the sensor back without traversing everything
                this.sensors[sensor_id] = location.add({ sensor_id, system_id, ...d });

            } catch (e) {
                this.logMessage(`Error adding sensor: ${e.message}`, 'error');
            }
        });
    }

    /**
     * Process a list of measurements
     *
     * @param {array} measurements - list of measurement data
     */
    async processMeasurementsData(measurements) {
        console.debug(`Processing ${measurements.length} measurements`);
        // if we provided a parameter column key we use that
        // otherwise we use the list of parameters
        let params = [];
        let long_format = false;

        if (measurements.length) {
            const keys = Object.keys(measurements[0]);
            long_format = keys.includes(this.parameter_key) && keys.includes(this.value_key);
            if (long_format) {
                params = [this.parameter_key];
            } else {
                params = Object.keys(this.parameters);
            }
        }

        measurements.map( (meas) => {
            try {
                const datetime = this.getDatetime(meas);
                const location = meas[this.location_key];
                params.map((p) => {
                    const value = long_format ? meas[this.value_key] : meas[p];
                    const metric = long_format ? meas[p] : p;
                    const m = {
                        location,
                        value,
                        metric,
                    };
                    if (m.value) {
                        this.measures.push({
                            sensor_id: this.getSensorId(m),
                            timestamp: datetime,
                            measure: this.normalize(m),
                        });
                    } else {
                        this.logMessage('VALUE_NOT_FOUND', 'error');
                    }
                });
            } catch (e) {
                this.logMessage('MEASUREMENT_ERROR', 'error', e);
            }
        });
    }

    /**
     * PLACEHOLDER
     *
     * @param {*} flags -
     * @returns {*} -
     */
    async processFlagsData(flags) {
        console.debug(`Processing ${flags.length} flags`);
        flags.map((d) => {
            try {

                const sensor = this.getSensor({
                    location: d[this.location_key],
                    metric: d[this.parameter_key],
                    ...d,
                });

                if(sensor) {
                    sensor.add(d);
                }

            } catch (e) {
                console.warn(`Error adding flag: ${e.message}`);
            }
        });
    }


    /**
     * Method to dump data to format that we can ingest
     *
     * @returns {object} - data object formated for ingestion
     */
    data() {
        return {
            meta: {
                schema: 'v0.1',
                source: this.provider,
                matching_method: 'ingest-id'
            },
            measures: this.measures.json(),
            locations: Object.values(this.locations).map((l)=>l.json())
        };
    }

    /**
     * Dump a summary that we can pass back to the log
     *
     * @returns {object} - json summary object
     */
    summary() {
        const error_summary = {};
        Object.keys(this.log).map((k) => error_summary[k] = this.log[k].length);
        return {
            source_name: this.provider,
            locations: Object.values(this.locations).length,
            systems: Object.values(this.locations).map((l) => Object.values(l.systems).length).flat().reduce((d,i) => d + i),
            sensors: Object.values(this.locations).map((l) => Object.values(l.systems).map((s) => Object.values(s.sensors).length)).flat().reduce((d,i) => d + i),
            // taking advantage of the sensor object list
            flags: Object.values(this.sensors).map((s) => Object.values(s.flags).length).flat().reduce((d,i) => d + i),
            measures: this.measures.length,
            errors: error_summary,
            from: this.measures.from && this.measures.from.utc().format(),
            to: this.measures.to && this.measures.to.utc().format(),
        };
    }
}

module.exports = {
    /**
     * Processor that is used by the fetch framework
     * This one assumes that data will be passed to it and its just reshaping
     * @param {*} source - json docuemnt that contains all the config
     * @returns {Promise<object>} - returns the summary information to be logged
     */
    async processor(source) {
        // create new client
        const client = new Client(source);
        // the lib method used to gather measurands is async
        // so we are doing it outside the constructor until we change that
        await client.fetchMeasurands();

        // files could be a temp name that represents data or list of data sources
        // if its a list/array then
        await Promise.all(source.files.map(async (file) => {
            await client.processData(file);
        }));
        // fetch and process the data
        // await client.fetchData();
        // and then push it to the
        // console.dir(client.data(), { depth: null });
        const file_name = DRYRUN ? 'test_data' : null;
        Providers.put_measures_json(client.provider, client.data(), file_name);
        return client.summary();
    },
    Client,
};
