/*
  Setup notes:
    - Need OAuth Service Account:
        - https://console.cloud.google.com/iam-admin/serviceaccounts
        - https://developers.google.com/identity/protocols/oauth2/service-account
*/

'use strict';
const { google, drive_v3 } = require('googleapis');
const csvParser = require('csv-parser');
const dayjs = require('dayjs')
    .extend(require('dayjs/plugin/utc'))
    .extend(require('dayjs/plugin/timezone'))
    .extend(require('dayjs/plugin/customParseFormat'));
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');
const { Measures, FixedMeasure } = require('../measure');
const { VERBOSE, fetchSecret } = require('../utils');

// dataKey: [ measurand_parameter, measurand_unit]
const lookup = {
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

async function processor(source_name, source) {
    /**
    The data are stored in a Google Drive folder at the following address:
    https://drive.google.com/drive/folders/1Mp_a-OyGGlk5tGkezYK41iZ2qybnrPzp?usp=sharing

    This folder contains subfolders categorized by month and year. Each subfolder
    contains CSV files stamped with the date and time.
    */
    const credentials = await fetchSecret(source.provider);

    // eslint-disable-next-line no-use-before-define
    const drive = new DriveFiles({
        auth: new google.auth.GoogleAuth({
            credentials,
            scopes: ['https://www.googleapis.com/auth/drive']
        })
    });

    // NOTE: Times are recorded in EST
    const now = dayjs().tz('America/New_York');
    const since = now
        .subtract(1, 'hour')
        .subtract(15, 'minute');
    console.log(`Looking for data between ${since.toISOString()} and ${now.toISOString()} (a ${now.diff(since, 'minute')} minute window)`);

    // Get folder for each month's readings
    if (VERBOSE) console.debug('Searching for directories with names matching the year/months within our search window...');
    const monthDirs = await drive.listAcrossPagination({
        q: `'${source.meta.folderId}' in parents AND (${getMonthQuery(since, now)})`
    });

    if (VERBOSE) console.debug(`Searching ${monthDirs.length} month directories for files to process...`);

    const stations = {};
    const measures = new Measures(FixedMeasure);

    // For whatever reason, we're unable to query Drive for multiple parents.
    // (e.g. `'abc123' in parents OR 'xyz987' in parents`). Instead, we'll
    // handle each month folder concurrently.
    await Promise.all(
        monthDirs
            .map(async (monthDir) => {
                if (VERBOSE) console.debug(`Searching for relevant files within '${monthDir.name}'...`);
                const files = await drive.listAcrossPagination(
                    { q: `'${monthDir.id}' in parents` },
                    getFilenameFromTimestamp(since)
                );
                console.info(`Found ${files.length} relevant files to process in '${monthDir.name}'...`);

                for (const file of files) {
                    const timestamp = getTimestampFromFilename(file.name);

                    const readings = await drive.parseCsv(file.id);
                    console.log(`Retrieved readings from ${readings.length} sensors in ${file.name}`);
                    for (const row of readings) {
                        const sensorNodeId = row.Anon_Name;
                        // Create station if does not already exist
                        if (!stations[sensorNodeId]) {
                            stations[sensorNodeId] = Providers.put_station(
                                source_name,
                                buildSensorNode(row)
                            );
                        }

                        // Register measurements
                        for (const lookupEntry of Object.entries(lookup)) {
                            // eslint-disable-next-line no-unused-vars
                            const [inputParam, [measurand_parameter, measurand_unit]] = lookupEntry;
                            const measure = row[inputParam];
                            if ([undefined, null, 'NaN'].includes(measure)) continue;
                            measures.push({
                                sensor_id: getSensorId(sensorNodeId, measurand_parameter),
                                measure,
                                timestamp: timestamp.toISOString()
                            });
                        }
                    }
                }
            })
    );

    await Object.values(stations);
    console.log('ok - all fixed stations pushed');

    await Providers.put_measures(source_name, measures);
    console.log('ok - all measures pushed');
}


/**
 * Return an array of years and months (represented as a string, formatted as
    such: 'YYYY-MM') that occurred between two times
 *
 * @param {dayjs.Dayjs} from Start time
 * @param {dayjs.Dayjs} to End time
 *
 * @returns {string}
 */
function getMonthQuery(from, to) {
    return [...Array(1 + to.diff(from, 'months')).keys()]
        .map((months) => from.add(months, 'months').format('YYYY-MM'))
        .map((yearMonth) => `name = '${yearMonth}'`)
        .join(' OR ');
}

/**
 * Return the expected filename of a CSV written at the provided timestamp.
 *
 * @param {dayjs.Dayjs} timestamp
 *
 * @returns {string}
 */
function getFilenameFromTimestamp(timestamp) {
    return `Location_Data ${timestamp.format('YYYY-MM-DD HH_MM')}.csv`;
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
function getTimestampFromFilename(filename) {
    const timestring = filename.slice('Location_Data '.length, 0 - '.csv'.length);
    const timestamp = dayjs(timestring, 'YYYY-MM-DD HH_mm', true);
    if (!timestamp.isValid()) {
        throw new Error(`Unable to parse ${filename} into valid timestamp.`);
    }
    return timestamp
        .subtract(15, 'minute')
        .tz('America/New_York', true);
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
 *
 * @param {CmuReading} reading
 *
 * @returns {SensorNode}
 */
function buildSensorNode(reading) {
    return new SensorNode({
        sensor_node_id: reading.Anon_Name,  // Needed for Provider Station key
        sensor_node_site_name: reading.Anon_Name,
        sensor_node_geometry: [reading.Lon, reading.Lat],
        sensor_node_source_name: 'CMU',
        sensor_node_ismobile: false,
        sensor_system: new SensorSystem({
            sensors: Object.values(lookup)
                .map(([measurand_parameter, measurand_unit]) => (
                    new Sensor({
                        sensor_id: getSensorId(reading.Anon_Name, measurand_parameter),
                        measurand_parameter,
                        measurand_unit
                    })
                ))
        })
    });
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
    processor
};

