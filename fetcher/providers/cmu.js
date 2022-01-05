const { google, drive_v3 } = require('googleapis');
const csvParser = require('csv-parser');
const dayjs = require('dayjs')
    .extend(require('dayjs/plugin/utc'))
    .extend(require('dayjs/plugin/timezone'))
    .extend(require('dayjs/plugin/customParseFormat'));
const pLimit = require('p-limit');
const Providers = require('../lib/providers');
const { Sensor, SensorNode, SensorSystem } = require('../lib/station');
const { Measures, FixedMeasure } = require('../lib/measure');
const { Measurand } = require('../lib/measurand');
const { VERBOSE, fetchSecret } = require('../lib/utils');
const { MetaDetails } = require('../lib/meta');

/**
The data is stored in a Google Drive folder at the following address:
https://drive.google.com/drive/folders/1Mp_a-OyGGlk5tGkezYK41iZ2qybnrPzp?usp=sharing

This folder contains subfolders categorized by month and year. Each subfolder
contains CSV files stamped with the date and time.
*/

const lookup = {
    // input_param: [measurand_parameter, measurand_unit]
    'CO': ['co', 'ppb'], // CO
    'NO': ['no', 'ppb'], // NO
    'NO2': ['no2', 'ppb'], // NO2
    'O3': ['o3', 'ppb'], // O3
    'P': ['pressure', 'hpa'], // Pressure
    'PM025': ['pm25', 'μg/m³'], // PM2
    'RH': ['relativehumidity', '%'], // RelativeHumidity
    'SO2': ['so2', 'ppb'], // SO2
    'T': ['temperature', 'c'] // Temperature
};

async function processor(source) {
    // https://developers.google.com/identity/protocols/oauth2/service-account
    const [
        measurands,
        credentials
    ] = await Promise.all([
        Measurand.getSupportedMeasurands(lookup),
        fetchSecret(source.provider)
    ]);

  const source_name = source.name;
    const meta = new MetaDetails(source_name);

    const drive = new DriveFiles({
        auth: new google.auth.GoogleAuth({
            credentials,
            scopes: ['https://www.googleapis.com/auth/drive']
        })
    });

    // NOTE: Times are recorded in EST
    const now = dayjs().tz('America/New_York');
    const since = dayjs(await meta.load() || '2019-03-01T00:00:00Z').tz('America/New_York');
    console.log(
        `Looking for data between ${since.toISOString()} and ${now.toISOString()} ` +
        `(a ${now.diff(since, 'hour', true).toFixed(2)} hour window)`
    );

    // Get folder for each month's readings
    if (VERBOSE) console.debug('Searching for directories with names matching the year/months within our search window...');
    const monthDirs = await drive.listAcrossPagination({
        q: `'${source.meta.folderId}' in parents AND (${getMonthQuery(since, now)})`
    });

    if (VERBOSE) console.debug(`Searching ${monthDirs.length} month directories for files to process...`);

    const stations = {};
    const filesBeingProcessed = [];
    let greatestTimestamp; // Keep track of the most recent file we process
    const limit = pLimit(10); // Limit to amount of files being processed at a given time

    // For whatever reason, we're unable to query Drive for multiple parents.
    // (e.g. `'abc123' in parents OR 'xyz987' in parents`). Instead, we'll
    // handle each month folder sequentually.
    for (const monthDir of monthDirs) {

        if (VERBOSE) console.debug(`Searching for relevant files within '${monthDir.name}'...`);
        const files = await drive.listAcrossPagination(
            { q: `'${monthDir.id}' in parents` },
            Filename.fromTimestamp(since)
        );

        console.info(`Found ${files.length} relevant files to process in '${monthDir.name}'...`);
        for (const file of files) {
            const timestamp = Filename.toTimestamp(file.name);
            if (greatestTimestamp === undefined || timestamp.toISOString() > greatestTimestamp) {
                greatestTimestamp = timestamp.toISOString();
            }

            filesBeingProcessed.push(
                limit(
                    () => processFile({ file, timestamp, stations, source_name, drive, measurands })
                )
            );
        }

    }

    await Promise.all(Object.values(stations));
    console.log(`ok - all ${Object.values(stations).length} fixed stations pushed`);

    await Promise.all(filesBeingProcessed);
    console.log(`ok - all ${filesBeingProcessed.length} files processed`);

    if (greatestTimestamp) {
        await meta.save(greatestTimestamp);
        console.log(`ok - recorded runtime marker ${greatestTimestamp}`);
    } else {
        console.log('warning - no timestamp found to save');
    }
}

/**
 * Return a query for years and months (represented as a string, formatted as
    such: 'YYYY-MM') that occurred between two times, inclusive
 *
 * @param {dayjs.Dayjs} from Start time
 * @param {dayjs.Dayjs} to End time
 *
 * @returns {string}
 */
function getMonthQuery(from, to) {
    const yearsApart = to.year() - from.year();
    const fromMonth = from.month();
    const toMonth = to.month() + (12 * yearsApart); // ensure toMonth is always greater than fromMonth in the event of year crossovers
    const monthsApart = toMonth - fromMonth;
    return [...Array(1 + monthsApart).keys()]
        .map((months) => from.add(months, 'months').format('YYYY-MM'))
        .map((yearMonth) => `name = '${yearMonth}'`)
        .join(' OR ');
}

async function processFile({ file, timestamp, stations, source_name, drive, measurands }) {
    const measures = new Measures(FixedMeasure);

    const readings = await drive.parseCsv(file.id);
    console.log(`Retrieved readings from ${readings.length} sensors in ${file.name}`);
    for (const row of readings) {
        const sensorNodeId = row.Anon_Name;
        // Create station if does not already exist
        if (!stations[sensorNodeId]) {
            stations[sensorNodeId] = Providers.put_station(
                source_name,
                new SensorNode({
                    sensor_node_id: row.Anon_Name,
                    sensor_node_site_name: row.Anon_Name,
                    sensor_node_geometry: [row.Lon, row.Lat],
                    sensor_node_source_name: 'CMU',
                    sensor_node_ismobile: false,
                    sensor_system: new SensorSystem({
                        sensors: measurands
                            .map((measurand) => (
                                new Sensor({
                                    sensor_id: getSensorId(row.Anon_Name, measurand.parameter),
                                    measurand_parameter: measurand.parameter,
                                    measurand_unit: measurand.normalized_unit
                                })
                            ))
                    })
                })
            );
        }

        // Register measurements
        for (const measurand of measurands) {
            const measure = row[measurand.input_param];
            if ([undefined, null, 'NaN'].includes(measure)) continue;
            measures.push({
                sensor_id: getSensorId(sensorNodeId, measurand.parameter),
                measure: measurand.normalize_value(measure),
                timestamp: timestamp.toISOString()
            });
        }
    }
    const filename = file.name.endsWith('.csv') ? file.name.slice(0, -4) : file.name;
    return Providers.put_measures(source_name, measures, filename);
}

class Filename {
    static prefix = 'Location_Data ';

    /**
     * Return the expected filename of a CSV written at the provided timestamp.
     *
     * @param {dayjs.Dayjs} timestamp
     *
     * @returns {string}
     */
    static fromTimestamp(timestamp) {
        return `${this.prefix}${timestamp.format('YYYY-MM-DD HH_MM')}.csv`;
    }

    /**
    * Converts a CMU formatted filename into a timestamp, accomodating for 15 minute
    * offset.  From notes:
    *
    * > Example: Location_Data 2020-07-17 11_45.csv
    * >
    * > Please note that the time stamp corresponds to the 15-minute average collected
    * > 15 minutes earlier. For example, the time stamp 11_45 corresponds to the average
    * > of data collected from 11:15 to 11:30.
    *
    * @param {String} filename
    *
    * @returns {dayjs.Dayjs}
    */
    static toTimestamp(filename) {
        const timestring = filename.slice(this.prefix.length, 0 - '.csv'.length);
        const timestamp = dayjs(timestring, 'YYYY-MM-DD HH_mm', true);
        if (!timestamp.isValid()) {
            throw new Error(`Unable to parse ${filename} into valid timestamp.`);
        }
        return timestamp
            .subtract(15, 'minute')
            .tz('America/New_York', true);
    }
}


/**
 * Convenience wrapper around Google Drive's Files API.
 */
class DriveFiles extends drive_v3.Resource$Files {

    constructor(options, google) {
        super({
            _options: options || {},
            google
        });
    }

    // eslint-disable-next-line valid-jsdoc
    /**
     * A pagination-friendly wrapper around the list() method.
     *
     * @param {import("googleapis").drive_v3.Params$Resource$Files$List} params
     * @param {String} [gte] Filter to stop parsing when file name is less than
     *  provided value.
     *
     * @returns {Promise<import("googleapis").drive_v3.Schema$File[]>}
     */
    async listAcrossPagination(params, gte) {
        let nextPageToken;
        const files = [];
        let page = 1;

        do {
            if (VERBOSE) console.debug(`Fetching page #${page} of ${JSON.stringify(params)}`);

            const { data } = await this.list({
                orderBy: 'name desc',
                ...params,
                pageToken: nextPageToken
            });

            for (const file of data.files) {
                if (gte && file.name < gte) {
                    if (VERBOSE) console.debug(`File '${file.name}' is less than '${gte}', no more files of interest.`);
                    return files;
                }

                files.push(file);
            }

            page++;
            nextPageToken = data.nextPageToken;
        } while (nextPageToken);

        return files;
    }

    /**
     * Retrieve rows from a CSV stored within Google Drive.
     *
     * @param {string} fileId
     *
     * @returns {Promise<CmuReading[]>}
     */
    async parseCsv(fileId) {
        const { data } = await this.get({
            fileId,
            alt: 'media'
        }, { responseType: 'stream' });

        return new Promise((resolve, reject) => {
            const rows = [];
            data
                .pipe(csvParser())
                .on('error', reject)
                .on('data', (row) => rows.push(row))
                .on('end', () => resolve(rows));
        });
    }
}


/**
 * Build sensor ID from a given sensor node ID and measurand parameter.
 *
 * @param {string} sensorNodeId
 * @param {string} measurandParameter
 *
 * @returns {string}
 */
function getSensorId(sensorNodeId, measurandParameter) {
    return `CMU-${sensorNodeId}-${measurandParameter}`;
}

/**
 * @typedef {Object} SensorParams
 *
 * @property {String} sensor_id
 * @property {String} measurand_parameter
 * @property {String} measurand_unit
 */

/**
 * @typedef {Object} CmuReading
 *
 * The csv file consists of 13 columns:
 * @property {String} Site_Name - InternalSiteName
 * @property {String} Anon_Name - AnonymizedSiteName
 * @property {String} Lat - Latitude [deg]
 * @property {String} Lon - Longitude [deg]
 * @property {String} CO - CO [ppb]
 * @property {String} NO - NO [ppb]
 * @property {String} NO2 - NO2 [ppb]
 * @property {String} O3 - O3 [ppb]
 * @property {String} P - Pressure [hPa]
 * @property {String} PM025 - PM2.5 [μg/m³]
 * @property {String} RH - RelativeHumidity [%]
 * @property {String} SO2 - SO2 [ppb]
 * @property {String} T - Temperature [°C]
 *
 * To protect the privacy of our hosts we ask that you do not publish column A
 * (Internal Site Name) and use column B (Anonymized Site Name) instead. For the
 * same reason, please round columns C and D (Lat and Lon) to the nearest 0.005
 * degrees (~250 m radius).
 *
 * Some of the fields will be populated with ‘NaN’ values. This is to indicate
 * that the site is:
 *   - Currently unmonitored
 *   - Not reporting data
 *   - Has suffered a failure of one or more sensors
 */

module.exports = {
    processor,
    getMonthQuery
};
